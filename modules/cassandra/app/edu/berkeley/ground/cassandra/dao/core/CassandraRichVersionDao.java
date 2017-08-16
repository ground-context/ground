/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p>
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.ground.cassandra.dao.core;

import edu.berkeley.ground.common.dao.core.RichVersionDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.exception.GroundException.ExceptionType;
import edu.berkeley.ground.common.model.core.RichVersion;
import edu.berkeley.ground.common.model.core.StructureVersion;
import edu.berkeley.ground.common.model.version.GroundType;
import edu.berkeley.ground.common.model.version.Tag;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.cassandra.dao.CqlConstants;
import edu.berkeley.ground.cassandra.dao.version.CassandraTagDao;
import edu.berkeley.ground.cassandra.dao.version.CassandraVersionDao;
import edu.berkeley.ground.cassandra.util.CassandraDatabase;
import edu.berkeley.ground.cassandra.util.CassandraStatements;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.QueryExecutionException;

// import java.sql.Connection; // Andre - what to do here
// import java.sql.ResultSet;
// import java.sql.SQLException;
// import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import play.Logger; // Not necessary
// import play.db.Database;

public abstract class CassandraRichVersionDao<T extends RichVersion> extends CassandraVersionDao<T> implements RichVersionDao<T> {

  private CassandraTagDao cassandraTagDao;

  public CassandraRichVersionDao(CassandraDatabase dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
    this.cassandraTagDao = new CassandraTagDao(dbSource);
  }

  @Override
  public CassandraStatements insert(final T richVersion) throws GroundException {
    long id = richVersion.getId();
    Long structureVersionId;

    if (richVersion.getStructureVersionId() == -1) {
      structureVersionId = null;
    } else {
      CassandraStructureVersionDao cassandraStructureVersionDao = new CassandraStructureVersionDao(dbSource, idGenerator);
      StructureVersion structureVersion = cassandraStructureVersionDao.retrieveFromDatabase(richVersion.getStructureVersionId());
      structureVersionId = richVersion.getStructureVersionId();
      checkStructureTags(structureVersion, richVersion.getTags());
    }

    CassandraStatements statements = super.insert(richVersion);

    String reference = richVersion.getReference();
    if (reference != null) {
      statements.append(String.format(CqlConstants.INSERT_RICH_VERSION_WITH_REFERENCE, id, structureVersionId, reference));
    } else {
      statements.append(String.format(CqlConstants.INSERT_RICH_VERSION_WITHOUT_REFERENCE, id, structureVersionId));
    }

    final Map<String, Tag> tags = richVersion.getTags();
    for (String tagKey : tags.keySet()) {
      Tag tag = tags.get(tagKey);

      statements.merge(this.cassandraTagDao.insertRichVersionTag(new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType())));
    }

    Map<String, String> parameters = richVersion.getParameters();
    if (!parameters.isEmpty()) {
      for (String key : parameters.keySet()) {
        statements.append(String.format(CqlConstants.INSERT_RICH_VERSION_EXTERNAL_PARAMETER, richVersion.getId(), key, parameters.get(key)));
      }
    }

    return statements; // Andre - What happens to these statements that are returned?
  }

  @Override
  public CassandraStatements delete(long id) {
    CassandraStatements statements = new CassandraStatements();

    statements.append(String.format(CqlConstants.DELETE_RICH_VERSION_TAGS, id));
    statements.append(String.format(CqlConstants.DELETE_RICH_EXTERNAL_PARAMETERS, id));
    statements.append(String.format(CqlConstants.DELETE_BY_ID, "rich_version", id));

    CassandraStatements superStatements = super.delete(id);
    superStatements.merge(statements);
    return superStatements;
  }


  @Override
  public RichVersion retrieveFromDatabase(long id) throws GroundException {
    String cql = String.format(CqlConstants.SELECT_STAR_BY_ID, "rich_version", id);

    // ResultSet resultSet;
    String reference;
    long structureVersionId;

    // Cluster cluster = this.dbSource.getCluster();
    // Session session = this.dbSource.getSession(cluster);

    Session session = this.dbSource.getSession();


    try {
      ResultSet resultSet = session.execute(cql);

      if (resultSet.isExhausted()) {
        throw new GroundException(ExceptionType.VERSION_NOT_FOUND, RichVersion.class.getSimpleName(), String.format("%d", id));
      }

      Row nextRow = resultSet.one();

      // Logger.debug("Andre: nextRow: " + nextRow.getColumnDefinitions().size());
      // for (int i = 0; i < nextRow.getColumnDefinitions().size(); i++) {
      //   Logger.debug("Andre: nextRow: " + nextRow.getColumnDefinitions().getName(i));
      // }

      reference = nextRow.getString("reference"); // Andre - Modified index
      structureVersionId = nextRow.getLong("structure_version_id"); // Andre - Modified index
      
    } catch (QueryExecutionException e) {
      throw new GroundException(e);
    }

    // session.close();
    // cluster.close();

    // try (Connection con = dbSource.getConnection()) {
    //   Statement stmt = con.createStatement();
    //   resultSet = stmt.executeQuery(cql);

    //   if (!resultSet.next()) {
    //     throw new GroundException(ExceptionType.VERSION_NOT_FOUND, RichVersion.class.getSimpleName(), String.format("%d", id));
    //   }

    //   reference = resultSet.getString(3);
    //   structureVersionId = resultSet.getLong(2);
    //   stmt.close();
    //   con.close();
    // } catch (SQLException e) { // Andre - WTF DO I DO HERE! CQLException????
    //   throw new GroundException(e);
    // }

    Map<String, Tag> tags = this.cassandraTagDao.retrieveFromDatabaseByVersionId(id);
    Map<String, String> referenceParams = getReferenceParameters(id);
    structureVersionId = structureVersionId == 0 ? -1 : structureVersionId;

    return new RichVersion(id, tags, structureVersionId, reference, referenceParams);
  }

  private Map<String, String> getReferenceParameters(long id) throws GroundException {
    String cql = String.format(CqlConstants.SELECT_RICH_VERSION_EXTERNAL_PARAMETERS, id);
    Map<String, String> referenceParameters = new HashMap<>();

    // Cluster cluster = this.dbSource.getCluster();
    // Session session = this.dbSource.getSession(cluster);
    
    Session session = this.dbSource.getSession();

    ResultSet parameterSet = session.execute(cql);
    // Row nextRow = parameterSet.one();
    if (parameterSet.isExhausted()) {
      return referenceParameters;
    }

    // referenceParameters.put(nextRow.getString("key"), nextRow.getString("value")); // Andre - Modified index
    for (Row row: parameterSet.all()) {
      referenceParameters.put(row.getString("key"), row.getString("value")); // Andre - Modified index
    }

    // session.close();
    // cluster.close();

    // try (Connection con = dbSource.getConnection()) {
    //   Statement stmt = con.createStatement();
    //   ResultSet parameterSet = stmt.executeQuery(cql);
    //   if (!parameterSet.next()) {
    //     return referenceParameters;
    //   }
    //   do {
    //     referenceParameters.put(parameterSet.getString(2), parameterSet.getString(3));
    //   } while (parameterSet.next());

    //   stmt.close();
    //   con.close();
    // } catch (Exception e) {
    //   throw new GroundException(e);
    // }
    return referenceParameters;
  }


  /**
   * Validate that the given Tags satisfy the StructureVersion's requirements.
   *
   * @param structureVersion the StructureVersion to check against
   * @param tags the provided tags
   */
  private static void checkStructureTags(StructureVersion structureVersion, Map<String, Tag> tags)
    throws GroundException {

    Map<String, GroundType> structureVersionAttributes = structureVersion.getAttributes();

    if (tags.isEmpty()) {
      throw new GroundException(ExceptionType.OTHER, String.format("No tags were specified even though a StructureVersion (%d) was.",
        structureVersion.getId()));
    }

    for (String key : structureVersionAttributes.keySet()) {
      if (!tags.keySet().contains(key)) {
        // check if such a tag exists
        throw new GroundException(ExceptionType.OTHER, String.format("No tag with key %s was specified.", key));
      } else if (tags.get(key).getValueType() == null) {
        // check that value type is specified
        throw new GroundException(ExceptionType.OTHER, String.format("Tag with key %s did not have a value.", key));
      } else if (!tags.get(key).getValueType().equals(structureVersionAttributes.get(key))) {
        // check that the value type is the same
        throw new GroundException(ExceptionType.OTHER, String.format(
          "Tag with key %s did not have a value of the correct type: expected [%s] but found [%s].", key, structureVersionAttributes.get(key),
          tags.get(key).getValueType()));
      }
    }
  }
}
