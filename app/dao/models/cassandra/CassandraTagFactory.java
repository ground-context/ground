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

package dao.models.cassandra;

import dao.models.TagFactory;
import db.CassandraClient;
import db.DbClient;
import db.DbCondition;
import db.DbEqualsCondition;
import db.DbResults;
import db.DbRow;
import exceptions.GroundException;
import models.models.Tag;
import models.versions.GroundType;

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

    List<DbCondition> predicates = new ArrayList<>();
    predicates.add(new DbEqualsCondition(keyPrefix + "_id", GroundType.LONG, id));

    Map<String, Tag> result = new HashMap<>();
    DbResults resultSet = this.dbClient.select(keyPrefix + "_tag",
        DbClient.SELECT_STAR, predicates);

    for (DbRow row : resultSet) {
      long fromId = row.getLong("from_rich_version_id");
      long toId = row.getLong("to_rich_version_id");
      String key = row.getString("key");

      // these methods will return null if the input is null, so there's no need to check
      GroundType type = GroundType.fromString(row.getString("type"));
      Object value = row.getValue(type, "value");

      result.put(key, new Tag(fromId, toId, key, value, type));
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
    List<DbCondition> predicates = new ArrayList<>();
    predicates.add(new DbEqualsCondition("key", GroundType.STRING, tag));

    List<String> projections = new ArrayList<>();
    String idColumn = keyPrefix + "_id";
    projections.add(idColumn);

    List<Long> result = new ArrayList<>();
    DbResults resultSet = this.dbClient.select(keyPrefix + "_tag",
        projections, predicates);

    for (DbRow row : resultSet) {
      result.add(row.getLong(idColumn));
    }

    return result;
  }
}
