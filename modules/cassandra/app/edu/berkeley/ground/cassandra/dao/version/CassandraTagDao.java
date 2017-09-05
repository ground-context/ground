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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    Session session = this.dbSource.getSession();

    ResultSet resultSet = session.execute(cql);
    for (Row row: resultSet.all()) {
      String key = row.getString("key");

      // these methods will return null if the input is null, so there's no need to check
      GroundType type = GroundType.fromString(row.getString("type"));
      Object value = this.getValue(type, row, "value");

      results.put(key, new Tag(id, key, value, type));
    }

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

    Session session = this.dbSource.getSession();

    try {
      ResultSet resultSet = session.execute(cql);
      for (Row row: resultSet.all()) {
        result.add(row.getLong(idColumn));
      }
    } catch (QueryExecutionException e) {
      throw new GroundException(e);
    }

    return result;
  }

  private Object getValue(GroundType type, Row row, String columnName) throws GroundException {

    if (type == null) {
      return null;
    }


    switch (type) {
      case STRING:
        return row.getString(columnName);
      case INTEGER:
        return Integer.valueOf(row.getString(columnName));
      case LONG:
        return Long.valueOf(row.getString(columnName));
      case BOOLEAN:
        return Boolean.valueOf(row.getString(columnName));
      default:
        // this should never happen because we've listed all types
        throw new GroundException(ExceptionType.OTHER, String.format("Unidentified type: %s", type));
    }
  }
}
