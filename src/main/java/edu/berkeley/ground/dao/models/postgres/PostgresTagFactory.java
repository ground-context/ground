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

package edu.berkeley.ground.dao.models.postgres;

import edu.berkeley.ground.dao.models.TagFactory;
import edu.berkeley.ground.db.DbClient;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.db.PostgresResults;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PostgresTagFactory implements TagFactory {
  private final PostgresClient dbClient;

  public PostgresTagFactory(PostgresClient dbClient) {
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

    PostgresResults resultSet;
    resultSet = this.dbClient.equalitySelect(keyPrefix + "_tag", DbClient.SELECT_STAR,
        predicates);

    if (resultSet.isEmpty()) {
      return new HashMap<>();
    }

    do {
      String key = resultSet.getString(2);

      // these methods will return null if the input is null, so there's no need to check
      GroundType type = GroundType.fromString(resultSet.getString(4));
      Object value = this.getValue(type, resultSet, 3);

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
    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("key", GroundType.STRING, tag));

    PostgresResults resultSet;
    resultSet = this.dbClient.equalitySelect(keyPrefix + "_tag", DbClient.SELECT_STAR,
        predicates);

    if (resultSet.isEmpty()) {
      return new ArrayList<>();
    }

    List<Long> result = new ArrayList<>();

    do {
      result.add(resultSet.getLong(1));
    } while (resultSet.next());

    return result;
  }

  private Object getValue(GroundType type, PostgresResults resultSet, int index)
      throws GroundException {

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
}
