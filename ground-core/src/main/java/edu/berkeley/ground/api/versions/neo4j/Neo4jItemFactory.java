/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.api.versions.neo4j;

import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.models.neo4j.Neo4jTagFactory;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.api.versions.Item;
import edu.berkeley.ground.api.versions.ItemFactory;
import edu.berkeley.ground.api.versions.VersionHistoryDAG;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.exceptions.GroundDBException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Neo4jItemFactory extends ItemFactory {
  private final Neo4jClient dbClient;
  private final Neo4jVersionHistoryDAGFactory versionHistoryDAGFactory;
  private final Neo4jTagFactory tagFactory;

  public Neo4jItemFactory(Neo4jClient dbClient,
                          Neo4jVersionHistoryDAGFactory versionHistoryDAGFactory,
                          Neo4jTagFactory tagFactory) {
    this.dbClient = dbClient;
    this.versionHistoryDAGFactory = versionHistoryDAGFactory;
    this.tagFactory = tagFactory;
  }

  public void insertIntoDatabase(long id, Map<String, Tag> tags) throws GroundDBException {
    for (String key : tags.keySet()) {
      Tag tag = tags.get(key);

      List<DbDataContainer> tagInsertion = new ArrayList<>();
      tagInsertion.add(new DbDataContainer("item_id", GroundType.LONG, id));
      tagInsertion.add(new DbDataContainer("tkey", GroundType.STRING, key));

      if (tag.getValue() != null) {
        tagInsertion.add(new DbDataContainer("value", GroundType.STRING, tag.getValue().toString()));
        tagInsertion.add(new DbDataContainer("type", GroundType.STRING, tag.getValueType().toString()));
      } else {
        tagInsertion.add(new DbDataContainer("value", GroundType.STRING, null));
        tagInsertion.add(new DbDataContainer("type", GroundType.STRING, null));
      }

      this.dbClient.addVertexAndEdge("ItemTag", tagInsertion, "ItemTagConnection", id, new ArrayList<>());
      tagInsertion.clear();
    }
  }

  public Item retrieveFromDatabase(long id) throws GroundDBException {
    return ItemFactory.construct(id, this.tagFactory.retrieveFromDatabaseByItemId(id));
  }

  public void update(long itemId, long childId, List<Long> parentIds) throws GroundDBException {
    // If a parent is specified, great. If it's not specified, then make it a child of EMPTY.
    if (parentIds.isEmpty()) {
      parentIds.add(itemId);
    }

    VersionHistoryDAG dag;
    try {
      dag = this.versionHistoryDAGFactory.retrieveFromDatabase(itemId);
    } catch (GroundDBException e) {
      if (!e.getMessage().contains("No results found for query")) {
        throw e;
      }

      dag = this.versionHistoryDAGFactory.create(itemId);
    }

    for (long parentId : parentIds) {
      if (parentId != itemId && !dag.checkItemInDag(parentId)) {
        String errorString = "Parent " + parentId + " is not in Item " + itemId + ".";

        throw new GroundDBException(errorString);
      }

      this.versionHistoryDAGFactory.addEdge(dag, parentId, childId, itemId);
    }
  }

  public List<Long> getLeaves(long itemId) throws GroundDBException {
    try {
      VersionHistoryDAG<?> dag = this.versionHistoryDAGFactory.retrieveFromDatabase(itemId);

      return dag.getLeaves();
    } catch (GroundDBException e) {
      if (!e.getMessage().contains("No results found for query")) {
        throw e;
      }

      return new ArrayList<>();
    }
  }

}
