/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.dao.models.cassandra;

import edu.berkeley.ground.dao.models.TagFactory;
import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.db.CassandraResults;
import edu.berkeley.ground.db.DbClient;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CassandraTagFactory implements TagFactory {
  private final CassandraClient dbClient;

  public CassandraTagFactory(CassandraClient dbClient) {
    this.dbClient = dbClient;
  }

  @Override
  public Map<String, Tag> retrieveFromDatabaseByVersionId(long id) throws GroundException {
    return this.retrieveFromDatabaseById(id, "rich_version");
  }

  @Override
  public Map<String, Tag> retrieveFromDatabaseByItemId(long id) throws GroundException {
    return this.retrieveFromDatabaseById(id, "item");
  }

  private Map<String, Tag> retrieveFromDatabaseById(long id, String keyPrefix)
      throws GroundException {

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer(keyPrefix + "_id", GroundType.LONG, id));

    Map<String, Tag> result = new HashMap<>();

    CassandraResults resultSet = this.dbClient.equalitySelect(keyPrefix + "_tag",
        DbClient.SELECT_STAR,
        predicates);

    if (resultSet.isEmpty()) {
      // this means that there are no tags
      return result;
    }

    do {
      String key = resultSet.getString("key");

      // these methods will return null if the input is null, so there's no need to check
      GroundType type = GroundType.fromString(resultSet.getString("type"));
      Object value = this.getValue(type, resultSet);

      result.put(key, new Tag(id, key, value, type));
    } while (resultSet.next());

    return result;
  }

  @Override
  public List<Long> getVersionIdsByTag(String tag) throws GroundException {
    return this.getIdsByTag(tag, "rich_version");
  }

  @Override
  public List<Long> getItemIdsByTag(String tag) throws GroundException {
    return this.getIdsByTag(tag, "item");
  }

  private List<Long> getIdsByTag(String tag, String keyPrefix) throws GroundException {
    List<Long> result = new ArrayList<>();

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("key", GroundType.STRING, tag));

    List<String> projections = new ArrayList<>();
    String idColumn = keyPrefix + "_id";
    projections.add(idColumn);

    CassandraResults resultSet = this.dbClient.equalitySelect(keyPrefix + "_tag",
        projections,
        predicates);

    if (resultSet.isEmpty()) {
      // this means that there are no tags
      return result;
    }

    do {
      result.add(resultSet.getLong(idColumn));
    } while (resultSet.next());

    return result;
  }

  private Object getValue(GroundType type, CassandraResults resultSet) throws GroundException {
    if (type == null) {
      return null;
    }

    switch (type) {
      case STRING:
        return resultSet.getString("value");
      case INTEGER:
        return resultSet.getInt("value");
      case LONG:
        return resultSet.getLong("value");
      case BOOLEAN:
        return resultSet.getBoolean("value");
      default:
        // this should never happen because we've listed all types
        throw new GroundException("Unidentified type: " + type);
    }
  }
}
