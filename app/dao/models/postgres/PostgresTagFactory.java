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

package dao.models.postgres;

import dao.models.TagFactory;
import db.DbClient;
import db.DbDataContainer;
import db.DbResults;
import db.DbRow;
import db.PostgresClient;
import exceptions.GroundException;
import models.models.Tag;
import models.versions.GroundType;

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

    DbResults resultSet = this.dbClient.equalitySelect(keyPrefix + "_tag",
        DbClient.SELECT_STAR, predicates);

    for (DbRow row : resultSet) {
      String key = row.getString("key");

      // these methods will return null if the input is null, so there's no need to check
      GroundType type = GroundType.fromString(row.getString("type"));
      Object value = row.getValue(type, "value");

      result.put(key, new Tag(id, key, value, type));
    }

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

    List<Long> result = new ArrayList<>();
    DbResults resultSet = this.dbClient.equalitySelect(keyPrefix + "_tag",
        DbClient.SELECT_STAR, predicates);

    for (DbRow row : resultSet) {
      result.add(row.getLong(keyPrefix + "id"));
    }

    return result;
  }
}
