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
package edu.berkeley.ground.cassandra.dao.version;

import edu.berkeley.ground.common.dao.version.TagDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.exception.GroundException.ExceptionType;
import edu.berkeley.ground.common.model.version.GroundType;
import edu.berkeley.ground.common.model.version.Tag;
import edu.berkeley.ground.cassandra.dao.CqlConstants;
import edu.berkeley.ground.cassandra.util.CassandraDatabase;
import edu.berkeley.ground.cassandra.util.CassandraStatements;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.QueryExecutionException;

// import java.sql.Connection; // Andre - what do to here?
// import java.sql.ResultSet;
// import java.sql.SQLException;
// import java.sql.Statement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
//import play.db.Database;
import play.Logger; // Andre unnecessary

public class CassandraTagDao implements TagDao {

  private CassandraDatabase dbSource;

  public CassandraTagDao(CassandraDatabase dbSource) {
    this.dbSource = dbSource;
  }

  @Override
  public CassandraStatements insertItemTag(final Tag tag) {
    List<String> cqlList = new ArrayList<>();
    if (tag.getValue() != null) {
      cqlList.add(String.format(CqlConstants.INSERT_ITEM_TAG_WITH_VALUE, tag.getId(), tag.getKey(), tag.getValue(), tag.getValueType()));
    } else {
      cqlList.add(
        String.format(CqlConstants.INSERT_ITEM_TAG_NO_VALUE, tag.getId(), tag.getKey()));
    }
    return new CassandraStatements(cqlList);
  }

  @Override
  public CassandraStatements insertRichVersionTag(final Tag tag) {
    List<String> cqlList = new ArrayList<>();
    if (tag.getValue() != null) {
      cqlList.add(String.format(CqlConstants.INSERT_RICH_VERSION_TAG_WITH_VALUE, tag.getId(), tag.getKey(), tag.getValue(), tag.getValueType()));
    } else {
      cqlList.add(String.format(CqlConstants.INSERT_RICH_VERSION_TAG_NO_VALUE, tag.getId(), tag.getKey()));
    }

    return new CassandraStatements(cqlList);
  }

  @Override
  public Map<String, Tag> retrieveFromDatabaseByVersionId(long id) throws GroundException {
    String cql = String.format(CqlConstants.SELECT_RICH_VERSION_TAGS, id);
    return this.retrieveFromDatabaseById(id, cql);
  }

  @Override
  public Map<String, Tag> retrieveFromDatabaseByItemId(long id) throws GroundException {
    String cql = String.format(CqlConstants.SELECT_ITEM_TAGS, id);
    return this.retrieveFromDatabaseById(id, cql);
  }

  private Map<String, Tag> retrieveFromDatabaseById(long id, String cql) throws GroundException {
    Map<String, Tag> results = new HashMap<>();

    // Cluster cluster = this.dbSource.getCluster();
    // Session session = this.dbSource.getSession(cluster);

    Session session = this.dbSource.getSession();
    // Logger.debug

    ResultSet resultSet = session.execute(cql);
    for (Row row: resultSet.all()) {
      String key = row.getString("key"); // Andre - Modified index

      // these methods will return null if the input is null, so there's no need to check
      GroundType type = GroundType.fromString(row.getString("type")); // Andre - Modified index
      Object value = this.getValue(type, row, "value"); // Andre - Modified index
      // Logger.debug("id: " + id);
      // Logger.debug("key: " + key);
      // Logger.debug("value: " + value);
      // Logger.debug("type: " + type);

      results.put(key, new Tag(id, key, value, type));
    }

    // session.close();
    // cluster.close();

    // try {
    //   Connection con = this.dbSource.getConnection();
    //   Statement stmt = con.createStatement();

    //   ResultSet resultSet = stmt.executeQuery(cql);

    //   while (resultSet.next()) {
    //     String key = resultSet.getString(2);

    //     // these methods will return null if the input is null, so there's no need to check
    //     GroundType type = GroundType.fromString(resultSet.getString(4));
    //     Object value = this.getValue(type, resultSet, 3);

    //     results.put(key, new Tag(id, key, value, type));
    //   }

      // stmt.close();
      // con.close();
    // } catch (SQLException e) { // Andre - CQLException????
      // throw new GroundException(e);
    // }

    return results;
  }

  @Override
  public List<Long> getVersionIdsByTag(String tag) throws GroundException {
    String cql = String.format(CqlConstants.SELECT_RICH_VERSION_TAGS_BY_KEY, tag);
    return this.getIdsByTag(cql, "rich_version_id");
  }

  @Override
  public List<Long> getItemIdsByTag(String tag) throws GroundException {
    String cql = String.format(CqlConstants.SELECT_ITEM_TAGS_BY_KEY, tag);
    return this.getIdsByTag(cql, "item_id");
  }

  private List<Long> getIdsByTag(String cql, String idColumn) throws GroundException {
    List<Long> result = new ArrayList<>();

    // Cluster cluster = this.dbSource.getCluster();
    // Session session = this.dbSource.getSession(cluster);

    Session session = this.dbSource.getSession();

    try {
      ResultSet resultSet = session.execute(cql);
      for (Row row: resultSet.all()) {
        result.add(row.getLong(idColumn)); // Andre - Modified
      }
    } catch (QueryExecutionException e) { // Andre - CQLException??
      throw new GroundException(e);
    }

    // session.close();
    // cluster.close();

    // try {
    //   Connection con = this.dbSource.getConnection();
    //   Statement stmt = con.createStatement();
    //   ResultSet resultSet = stmt.executeQuery(cql);

    //   while (resultSet.next()) {
    //     result.add(resultSet.getLong(1));
    //   }

    // } catch (SQLException e) { // Andre - CQLException??
    //   throw new GroundException(e);
    // }

    return result;
  }

  // private Object getValue(GroundType type, ResultSet resultSet, String columnName) throws GroundException, SQLException { // Andre - CQLException??
  private Object getValue(GroundType type, Row row, String columnName) throws GroundException { // Andre - CQLException??

    if (type == null) {
      return null;
    }

    Logger.debug(type.toString());
    Logger.debug(row.getString(columnName));

    switch (type) {
      case STRING:
        // return row.getString(columnName);
        return row.getString(columnName); // Andre
      case INTEGER:
        // return row.getInt(columnName);
        return Integer.valueOf(row.getString(columnName)); // Andre
      case LONG:
        // return row.getLong(columnName);
        return Long.valueOf(row.getString(columnName)); // Andre
      case BOOLEAN:
        // return row.getBool(columnName);
        return Boolean.valueOf(row.getString(columnName)); // Andre
      default:
        // this should never happen because we've listed all types
        throw new GroundException(ExceptionType.OTHER, String.format("Unidentified type: %s", type));
    }
  }
}
