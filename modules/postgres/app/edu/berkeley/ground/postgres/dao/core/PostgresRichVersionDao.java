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
package edu.berkeley.ground.postgres.dao.core;

import edu.berkeley.ground.common.dao.core.RichVersionDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.exception.GroundException.ExceptionType;
import edu.berkeley.ground.common.model.core.RichVersion;
import edu.berkeley.ground.common.model.core.StructureVersion;
import edu.berkeley.ground.common.model.version.GroundType;
import edu.berkeley.ground.common.model.version.Tag;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.SqlConstants;
import edu.berkeley.ground.postgres.dao.version.PostgresTagDao;
import edu.berkeley.ground.postgres.dao.version.PostgresVersionDao;
import edu.berkeley.ground.postgres.util.PostgresStatements;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import play.db.Database;

public abstract class PostgresRichVersionDao<T extends RichVersion> extends PostgresVersionDao<T> implements RichVersionDao<T> {

  private PostgresTagDao postgresTagDao;

  public PostgresRichVersionDao(Database dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
    this.postgresTagDao = new PostgresTagDao(dbSource);
  }

  @Override
  public PostgresStatements insert(final T richVersion) throws GroundException {
    long id = richVersion.getId();
    Long structureVersionId;

    if (richVersion.getStructureVersionId() == -1) {
      structureVersionId = null;
    } else {
      PostgresStructureVersionDao postgresStructureVersionDao = new PostgresStructureVersionDao(dbSource, idGenerator);
      StructureVersion structureVersion = postgresStructureVersionDao.retrieveFromDatabase(richVersion.getStructureVersionId());
      structureVersionId = richVersion.getStructureVersionId();
      checkStructureTags(structureVersion, richVersion.getTags());
    }

    PostgresStatements statements = super.insert(richVersion);
    statements.append(String.format(SqlConstants.INSERT_RICH_VERSION, id, structureVersionId, richVersion.getReference()));

    final Map<String, Tag> tags = richVersion.getTags();
    for (String tagKey : tags.keySet()) {
      Tag tag = tags.get(tagKey);

      statements.merge(this.postgresTagDao.insertRichVersionTag(new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType())));
    }

    Map<String, String> parameters = richVersion.getParameters();
    if (!parameters.isEmpty()) {
      for (String key : parameters.keySet()) {
        statements.append(String.format(SqlConstants.INSERT_RICH_VERSION_EXTERNAL_PARAMETER, richVersion.getId(), key, parameters.get(key)));
      }
    }

    return statements;
  }

  @Override
  public PostgresStatements delete(long id) {
    PostgresStatements statements = new PostgresStatements();

    statements.append(String.format(SqlConstants.DELETE_RICH_VERSION_TAGS, id));
    statements.append(String.format(SqlConstants.DELETE_RICH_EXTERNAL_PARAMETERS, id));
    statements.append(String.format(SqlConstants.DELETE_BY_ID, "rich_version", id));

    PostgresStatements superStatements = super.delete(id);
    superStatements.merge(statements);
    return superStatements;
  }


  @Override
  public RichVersion retrieveFromDatabase(long id) throws GroundException {
    String sql = String.format(SqlConstants.SELECT_STAR_BY_ID, "rich_version", id);

    ResultSet resultSet;
    String reference;
    long structureVersionId;

    try (Connection con = dbSource.getConnection()) {
      Statement stmt = con.createStatement();
      resultSet = stmt.executeQuery(sql);

      if (!resultSet.next()) {
        throw new GroundException(ExceptionType.VERSION_NOT_FOUND, RichVersion.class.getSimpleName(), String.format("%d", id));
      }

      reference = resultSet.getString(3);
      structureVersionId = resultSet.getLong(2);
      stmt.close();
      con.close();
    } catch (SQLException e) {
      throw new GroundException(e);
    }

    Map<String, Tag> tags = this.postgresTagDao.retrieveFromDatabaseByVersionId(id);
    Map<String, String> referenceParams = getReferenceParameters(id);
    structureVersionId = structureVersionId == 0 ? -1 : structureVersionId;

    return new RichVersion(id, tags, structureVersionId, reference, referenceParams);
  }

  private Map<String, String> getReferenceParameters(long id) throws GroundException {
    String sql = String.format(SqlConstants.SELECT_RICH_VERSION_EXTERNAL_PARAMETERS, id);
    Map<String, String> referenceParameters = new HashMap<>();

    try (Connection con = dbSource.getConnection()) {
      Statement stmt = con.createStatement();
      ResultSet parameterSet = stmt.executeQuery(sql);
      if (!parameterSet.next()) {
        return referenceParameters;
      }
      do {
        referenceParameters.put(parameterSet.getString(2), parameterSet.getString(3));
      } while (parameterSet.next());

      stmt.close();
      con.close();
    } catch (Exception e) {
      throw new GroundException(e);
    }
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
