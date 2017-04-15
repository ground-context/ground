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

package edu.berkeley.ground.dao.models.neo4j;

import edu.berkeley.ground.dao.models.TagFactory;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.exceptions.GroundDbException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.internal.value.NullValue;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.v1.Record;

public class Neo4jTagFactory implements TagFactory {
  private final Neo4jClient dbClient;

  public Neo4jTagFactory(Neo4jClient dbClient) {
    this.dbClient = dbClient;
  }

  @Override
  public Map<String, Tag> retrieveFromDatabaseByVersionId(long id) throws GroundException {
    return this.retrieveFromDatabaseById(id, "RichVersion");
  }

  @Override
  public Map<String, Tag> retrieveFromDatabaseByItemId(long id) throws GroundException {
    return this.retrieveFromDatabaseById(id, "Item");
  }

  private Map<String, Tag> retrieveFromDatabaseById(long id, String keyPrefix)
      throws GroundException {

    List<String> returnFields = new ArrayList<>();
    returnFields.add("tkey");
    returnFields.add("value");
    returnFields.add("type");

    List<Record> tagsRecords = this.dbClient.getAdjacentVerticesByEdgeLabel(keyPrefix
        + "TagConnection", id, returnFields);

    Map<String, Tag> tags = new HashMap<>();

    for (Record record : tagsRecords) {
      String key = Neo4jClient.getStringFromValue((StringValue) record.get("tkey"));

      Object value;
      if (record.containsKey("value") && !(record.get("value") instanceof NullValue)) {
        value = Neo4jClient.getStringFromValue((StringValue) record.get("value"));
      } else {
        value = null;
      }

      GroundType type;
      if (record.containsKey("type") && !(record.get("type") instanceof NullValue)) {
        type = GroundType.fromString(
            Neo4jClient.getStringFromValue((StringValue) record.get("type")));
        value = GroundType.stringToType((String) value, type);
      } else {
        type = null;
      }

      tags.put(key, new Tag(id, key, value, type));
    }

    return tags;
  }

  @Override
  public List<Long> getVersionIdsByTag(String tag) throws GroundDbException {
    return this.getIdsByTag(tag, "rich_version_id");
  }

  @Override
  public List<Long> getItemIdsByTag(String tag) throws GroundDbException {
    return this.getIdsByTag(tag, "item_id");
  }

  private List<Long> getIdsByTag(String tag, String idAttribute) throws GroundDbException {
    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("tkey", GroundType.STRING, tag));

    return this.dbClient.getVerticesByAttributes(predicates, idAttribute);
  }
}
