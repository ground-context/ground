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

package edu.berkeley.ground.dao.versions.cassandra;

import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.dao.models.cassandra.CassandraTagFactory;
import edu.berkeley.ground.model.versions.Item;
import edu.berkeley.ground.dao.versions.ItemFactory;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.model.versions.VersionHistoryDAG;
import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.exceptions.GroundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CassandraItemFactory extends ItemFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraItemFactory.class);

  private final CassandraClient dbClient;
  private final CassandraVersionHistoryDAGFactory versionHistoryDAGFactory;
  private final CassandraTagFactory tagFactory;

  public CassandraItemFactory(CassandraClient dbClient,
                              CassandraVersionHistoryDAGFactory versionHistoryDAGFactory,
                              CassandraTagFactory tagFactory) {
    this.dbClient = dbClient;
    this.versionHistoryDAGFactory = versionHistoryDAGFactory;
    this.tagFactory = tagFactory;
  }

  public void insertIntoDatabase(long id, Map<String, Tag> tags) throws GroundException {
    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("id", GroundType.LONG, id));

    this.dbClient.insert("item", insertions);

    for (String key : tags.keySet()) {
      Tag tag = tags.get(key);

      List<DbDataContainer> tagInsertion = new ArrayList<>();
      tagInsertion.add(new DbDataContainer("item_id", GroundType.LONG, id));
      tagInsertion.add(new DbDataContainer("key", GroundType.STRING, key));

      if (tag.getValue() != null) {
        tagInsertion.add(new DbDataContainer("value", GroundType.STRING, tag.getValue().toString()));
        tagInsertion.add(new DbDataContainer("type", GroundType.STRING, tag.getValueType().toString()));
      } else {
        tagInsertion.add(new DbDataContainer("value", GroundType.STRING, null));
        tagInsertion.add(new DbDataContainer("type", GroundType.STRING, null));
      }

      this.dbClient.insert("item_tag", tagInsertion);
    }
  }

  public Item retrieveFromDatabase(long id) throws GroundException {
    return ItemFactory.construct(id, this.tagFactory.retrieveFromDatabaseByItemId(id));
  }

  // TODO: Refactor logic for parent into function in ItemFactory
  public void update(long itemId, long childId, List<Long> parentIds) throws GroundException {
    // If a parent is specified, great. If it's not specified, then make it a child of EMPTY, which is version 0.
    if (parentIds.isEmpty()) {
      parentIds.add(0L);
    }

    VersionHistoryDAG dag;
    try {
      dag = this.versionHistoryDAGFactory.retrieveFromDatabase(itemId);
    } catch (GroundException e) {
      if (!e.getMessage().contains("No VersionHistoryDAG for Item")) {
        throw e;
      }

      dag = this.versionHistoryDAGFactory.create(itemId);
    }

    for (long parentId : parentIds) {
      if (parentId != 0 && !dag.checkItemInDag(parentId)) {
        String errorString = "Parent " + parentId + " is not in Item " + itemId + ".";

        LOGGER.error(errorString);
        throw new GroundException(errorString);
      }

      this.versionHistoryDAGFactory.addEdge(dag, parentId, childId, itemId);
    }
  }

  public List<Long> getLeaves(long itemId) throws GroundException {
    try {
      VersionHistoryDAG<?> dag = this.versionHistoryDAGFactory.retrieveFromDatabase(itemId);

      return dag.getLeaves();
    } catch (GroundException e) {
      if (!e.getMessage().contains("No results found for query:")) {
        throw e;
      }

      return new ArrayList<>();
    }
  }
}
