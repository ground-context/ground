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

package edu.berkeley.ground.dao.versions.neo4j;

import edu.berkeley.ground.dao.models.neo4j.Neo4jTagFactory;
import edu.berkeley.ground.dao.versions.ItemFactory;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.exceptions.GroundDbException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.model.versions.Item;
import edu.berkeley.ground.model.versions.VersionHistoryDag;
import edu.berkeley.ground.util.ElasticSearch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Neo4jItemFactory extends ItemFactory {
  private final Neo4jClient dbClient;
  private final Neo4jVersionHistoryDagFactory versionHistoryDagFactory;
  private final Neo4jTagFactory tagFactory;

  /**
   * Constructor for the Neo4j item factory.
   *
   * @param dbClient the Neo4j client
   * @param versionHistoryDagFactory the singleton Neo4jVersionHistoryDagFactory
   * @param tagFactory the singleton Neo4jTagFactory
   */
  public Neo4jItemFactory(Neo4jClient dbClient,
                          Neo4jVersionHistoryDagFactory versionHistoryDagFactory,
                          Neo4jTagFactory tagFactory) {
    this.dbClient = dbClient;
    this.versionHistoryDagFactory = versionHistoryDagFactory;
    this.tagFactory = tagFactory;
  }

  /**
   * Insert item information into the database.
   *
   * @param id the id of the item
   * @param tags the tags associated with the item
   * @throws GroundDbException an error inserting data into the database
   */
  @Override
  public void insertIntoDatabase(long id, Map<String, Tag> tags) throws GroundException {
    for (String key : tags.keySet()) {
      Tag tag = tags.get(key);
      ElasticSearch.insertElasticSearch(tag, "item");

      List<DbDataContainer> tagInsertion = new ArrayList<>();
      tagInsertion.add(new DbDataContainer("item_id", GroundType.LONG, id));
      tagInsertion.add(new DbDataContainer("tkey", GroundType.STRING, key));

      if (tag.getValue() != null) {
        tagInsertion.add(new DbDataContainer("value", GroundType.STRING,
            tag.getValue().toString()));
        tagInsertion.add(new DbDataContainer("type", GroundType.STRING,
            tag.getValueType().toString()));
      } else {
        tagInsertion.add(new DbDataContainer("value", GroundType.STRING, null));
        tagInsertion.add(new DbDataContainer("type", GroundType.STRING, null));
      }

      this.dbClient.addVertexAndEdge("ItemTag", tagInsertion, "ItemTagConnection", id,
          new ArrayList<>());
      tagInsertion.clear();
    }
  }

  /**
   * Retrieve Item information from the database.
   *
   * @param id the id of the item
   * @return the retrieved item
   * @throws GroundException either the item doesn't exist or couldn't be retrieved
   */
  @Override
  public Item retrieveFromDatabase(long id) throws GroundException {
    return ItemFactory.construct(id, this.tagFactory.retrieveFromDatabaseByItemId(id));
  }

  /**
   * Update this Item with a new version.
   *
   * @param itemId the id of the Item we're updating
   * @param childId the new version's id
   * @param parentIds the ids of the parents of the child
   * @throws GroundException an error
   */
  @Override
  public void update(long itemId, long childId, List<Long> parentIds) throws GroundException {
    // If a parent is specified, great. If it's not specified, then make it a child of EMPTY.
    if (parentIds.isEmpty()) {
      parentIds.add(itemId);
    }

    VersionHistoryDag dag;
    try {
      dag = this.versionHistoryDagFactory.retrieveFromDatabase(itemId);
    } catch (GroundDbException e) {
      if (!e.getMessage().contains("No results found for query")) {
        throw e;
      }

      dag = this.versionHistoryDagFactory.create(itemId);
    }

    for (long parentId : parentIds) {
      if (parentId != itemId && !dag.checkItemInDag(parentId)) {
        String errorString = "Parent " + parentId + " is not in Item " + itemId + ".";

        throw new GroundDbException(errorString);
      }

      this.versionHistoryDagFactory.addEdge(dag, parentId, childId, itemId);
    }
  }

  /**
   * Return the list of leaves of this item's DAG.
   *
   * @param itemId the id of the item
   * @return the list of leaves contained within the item
   * @throws GroundException an error retrieving the item
   */
  public List<Long> getLeaves(long itemId) throws GroundException {
    try {
      VersionHistoryDag<?> dag = this.versionHistoryDagFactory.retrieveFromDatabase(itemId);

      return dag.getLeaves();
    } catch (GroundDbException e) {
      if (!e.getMessage().contains("No results found for query")) {
        throw e;
      }

      return new ArrayList<>();
    }
  }

  /**
   * Truncate the item to only have the most recent levels.
   *
   * @param numLevels the levels to keep
   * @param itemType the type of the item to truncate
   * @throws GroundException an error while removing versions
   */
  @Override
  public void truncate(long itemId, int numLevels, String itemType) throws GroundException {
    VersionHistoryDag<?> dag = this.versionHistoryDagFactory.retrieveFromDatabase(itemId);

    this.versionHistoryDagFactory.truncate(dag, numLevels, itemType);
  }
}
