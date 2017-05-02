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

package dao.versions.cassandra;

import dao.models.cassandra.CassandraTagFactory;
import dao.versions.ItemFactory;
import db.CassandraClient;
import db.CassandraResults;
import db.DbDataContainer;
import exceptions.GroundException;
import exceptions.GroundItemNotFoundException;
import models.models.Tag;
import models.versions.GroundType;
import models.versions.Item;
import models.versions.VersionHistoryDag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.ElasticSearch;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class CassandraItemFactory<T extends Item> implements ItemFactory<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraItemFactory.class);

  private final CassandraClient dbClient;
  private final CassandraVersionHistoryDagFactory versionHistoryDagFactory;
  private final CassandraTagFactory tagFactory;

  /**
   * Constructor for the Cassandra item factory.
   *
   * @param dbClient the Cassandra client
   * @param versionHistoryDagFactory the singleton CassandraVersionHistoryDagFactory
   * @param tagFactory the singleton CassandraTagFactory
   */
  public CassandraItemFactory(CassandraClient dbClient,
                              CassandraVersionHistoryDagFactory versionHistoryDagFactory,
                              CassandraTagFactory tagFactory) {
    this.dbClient = dbClient;
    this.versionHistoryDagFactory = versionHistoryDagFactory;
    this.tagFactory = tagFactory;
  }

  /**
   * Insert item information into the database.
   *
   * @param id the id of the item
   * @param tags the tags associated with the item
   * @throws GroundException an error inserting data into the database
   */
  public void insertIntoDatabase(long id, Map<String, Tag> tags) throws GroundException {
    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("id", GroundType.LONG, id));

    this.dbClient.insert("item", insertions);

    for (String key : tags.keySet()) {
      Tag tag = tags.get(key);
      ElasticSearch.insertElasticSearch(tag, "item");
      List<DbDataContainer> tagInsertion = new ArrayList<>();
      tagInsertion.add(new DbDataContainer("item_id", GroundType.LONG, id));
      tagInsertion.add(new DbDataContainer("key", GroundType.STRING, key));

      if (tag.getValue() != null) {
        tagInsertion.add(new DbDataContainer("value", GroundType.STRING,
            tag.getValue().toString()));
        tagInsertion.add(new DbDataContainer("type", GroundType.STRING,
            tag.getValueType().toString()));
      } else {
        tagInsertion.add(new DbDataContainer("value", GroundType.STRING, null));
        tagInsertion.add(new DbDataContainer("type", GroundType.STRING, null));
      }

      this.dbClient.insert("item_tag", tagInsertion);
    }
  }

  /**
   * Retrieve the tags associated with a particular item id.
   *
   * @param id the id of the item
   * @return the tags associated with the item
   * @throws GroundException an error while retrieving the tags
   */
  protected Map<String, Tag> retrieveItemTags(long id) throws GroundException {
    return this.tagFactory.retrieveFromDatabaseByItemId(id);
  }

  /**
   * Update this Item with a new version.
   *
   * @param itemId the id of the Item we're updating
   * @param childId the new version's id
   * @param parentIds the ids of the parents of the child
   * @throws GroundException an error
   */
  public void update(long itemId, long childId, List<Long> parentIds) throws GroundException {
    // TODO: Refactor logic for parent into function in ItemFactory
    // If a parent is specified, great. If it's not specified, then make it a child of EMPTY,
    // which is version 0.
    if (parentIds.isEmpty()) {
      parentIds.add(0L);
    }

    VersionHistoryDag dag;
    try {
      dag = this.versionHistoryDagFactory.retrieveFromDatabase(itemId);
    } catch (GroundException e) {
      if (!e.getMessage().contains("No VersionHistoryDAG for Item")) {
        throw e;
      }

      dag = this.versionHistoryDagFactory.create(itemId);
    }

    for (long parentId : parentIds) {
      if (parentId != 0 && !dag.checkItemInDag(parentId)) {
        String errorString = "Parent " + parentId + " is not in Item " + itemId + ".";

        LOGGER.error(errorString);
        throw new GroundException(errorString);
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
    } catch (GroundException e) {
      if (!e.getMessage().contains("No results found for query:")) {
        throw e;
      }

      return new ArrayList<>();
    }
  }

  /**
   * Truncate the item to only have the most recent levels.
   *
   * @param numLevels the levels to keep
   * @throws GroundException an error while removing versions
   */
  @Override
  public void truncate(long itemId, int numLevels) throws GroundException {
    VersionHistoryDag<?> dag = this.versionHistoryDagFactory.retrieveFromDatabase(itemId);

    this.versionHistoryDagFactory.truncate(dag, numLevels, this.getType());
  }

  /**
   * Verify that a result set for an item is not empty.
   *
   * @param resultSet the result set to check
   * @param fieldName the name of the field that was used to retrieve this item
   * @param value the value used to retrieve the item
   * @throws GroundItemNotFoundException an exception indicating the item wasn't found
   */
  protected void verifyResultSet(CassandraResults resultSet, String fieldName, Object value)
    throws GroundItemNotFoundException {

    if (resultSet.isEmpty()) {
      throw new GroundItemNotFoundException(this.getType(), fieldName, value);
    }
  }
}
