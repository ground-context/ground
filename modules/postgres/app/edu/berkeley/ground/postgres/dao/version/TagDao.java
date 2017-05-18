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
package edu.berkeley.ground.postgres.dao.version;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.factory.version.TagFactory;
import edu.berkeley.ground.common.model.version.GroundType;
import edu.berkeley.ground.common.model.version.Tag;
import edu.berkeley.ground.common.utils.IdGenerator;
import edu.berkeley.ground.postgres.utils.PostgresStatements;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import play.db.Database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TagDao implements TagFactory {

  Database dbSource;
  IdGenerator idGenerator;

  public TagDao(Database dbSource, IdGenerator idGenerator) {
    this.dbSource = dbSource;
    this.idGenerator = idGenerator;
  }

  public final void create(final Database dbSource, final Tag tag, final IdGenerator idGenerator) throws GroundException {
    long uniqueId = idGenerator.generateItemId();
    Tag newTag = new Tag(uniqueId, tag.getKey(), tag.getValue(), tag.getValueType());
    try {
      PostgresUtils.executeSqlList(dbSource, insert(newTag));
    } catch (Exception e) {
      throw new GroundException(e);
    }
  }

  public PostgresStatements insert(final Tag tag) {
    List<String> sqlList = new ArrayList<>();
    sqlList.add(String.format("insert into item_tag (item_id, key, value, type) values (%d, '%s', '%s', '%s')",
      tag.getId(), tag.getKey(), tag.getValue(), tag.getValueType()));
    return new PostgresStatements(sqlList);
  }

  public Map<String, Tag> retrieveFromDatabaseById(long id, String prefix) throws GroundException, SQLException {

    Connection con = dbSource.getConnection();
    Statement stmt = con.createStatement();
    String sql = String.format("select * from %s_tag where %s_id = %d", prefix, prefix, id);
    ResultSet resultSet = stmt.executeQuery(sql);

    Map<String, Tag> results = new HashMap<>();

    do {
      String key = resultSet.getString(2);

      // these methods will return null if the input is null, so there's no need to check
      GroundType type = GroundType.fromString(resultSet.getString(4));
      Object value = this.getValue(type, resultSet, 3);

      results.put(key, new Tag(id, key, value, type));
    } while (resultSet.next());

    return results;
  }

  private Object getValue(GroundType type, ResultSet resultSet, int index)
    throws GroundException, SQLException {

    if (type == null) {
      return null;
    }

    switch (type) {
      case STRING:
        return resultSet.getString(index);
      case INTEGER:
        return resultSet.getInt(index);
      case LONG:
        return resultSet.getLong(index);
      case BOOLEAN:
        return resultSet.getBoolean(index);
      default:
        // this should never happen because we've listed all types
        throw new GroundException("Unidentified type: " + type);
    }
  }

  @Override
  public List<Long> getVersionIdsByTag(String tag) throws GroundException {
    return null;
  }

  @Override
  public List<Long> getItemIdsByTag(String tag) throws GroundException {
    return null;
  }
}
