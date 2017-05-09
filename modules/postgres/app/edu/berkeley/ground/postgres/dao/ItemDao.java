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
package edu.berkeley.ground.postgres.dao;

import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.factory.version.ItemFactory;
import edu.berkeley.ground.lib.factory.version.TagFactory;
import edu.berkeley.ground.lib.factory.version.VersionHistoryDagFactory;
import edu.berkeley.ground.lib.model.version.Item;
import edu.berkeley.ground.lib.model.version.Tag;
import edu.berkeley.ground.lib.model.version.VersionHistoryDag;
import edu.berkeley.ground.lib.utils.IdGenerator;
import edu.berkeley.ground.postgres.utils.PostgresClient;
import edu.berkeley.ground.postgres.utils.PostgresUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import play.db.Database;

public class ItemDao<T extends Item> implements ItemFactory<T> {
  private PostgresClient dbClient;
  private VersionHistoryDagDao versionHistoryDagDao;
  private TagFactory tagFactory;

  public ItemDao() {}

  public ItemDao(PostgresClient dbClient,
                             VersionHistoryDagDao versionHistoryDagDao,
                             TagFactory tagFactory) {
    this.dbClient = dbClient;
    this.versionHistoryDagDao = versionHistoryDagDao;
    this.tagFactory = tagFactory;
  }

  @Override
  public T retrieveFromDatabase(Database dbSource, long id) throws GroundException {
    return null;
  }

  @Override
  public T retrieveFromDatabase(Database dbSource, String sourceKey) throws GroundException {
    return null;
  }

  @Override
  public Class<T> getType() {
    return null;
  }

  @Override
  public List<Long> getLeaves(Database dbSource, long itemId) throws GroundException {
    try {
      VersionHistoryDag<?> dag = this.versionHistoryDagDao.retrieveFromDatabase(itemId);

      return dag.getLeaves();
    } catch (GroundException e) {
      if (!e.getMessage().contains("No results found for query:")) {
        throw e;
      }

      return new ArrayList<>();
    }
  }

  /**
   * Add a new Version to this Item. The provided parentIds will be the parents of this particular
   * version. What's provided in the default case varies based on which database we are writing
   * into.
   *
   * @param itemId the id of the Item we're updating
   * @param childId the new version's id
   * @param parentIds the ids of the parents of the child
   */
  @Override
  public void update(long itemId, long childId, List<Long> parentIds) throws GroundException {
    // If a parent is specified, great. If it's not specified, then make it a child of EMPTY.
    if (parentIds.isEmpty()) {
      parentIds.add(0L);
    }

    VersionHistoryDag dag;
    try {
      dag = this.versionHistoryDagDao.retrieveFromDatabase(itemId);
    } catch (GroundException e) {
      if (!e.getMessage().contains("No results found for query:")) {
        throw e;
      }

      dag = this.versionHistoryDagDao.create(itemId);
    }

    for (long parentId : parentIds) {
      if (parentId != 0 && !dag.checkItemInDag(parentId)) {
        String errorString = "Parent " + parentId + " is not in Item " + itemId + ".";
        throw new GroundException(errorString);
      }

      this.versionHistoryDagDao.addEdge(dag, parentId, childId, itemId);
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
    VersionHistoryDag<?> dag = versionHistoryDagDao.retrieveFromDatabase(itemId);
    this.versionHistoryDagDao.truncate(dag, numLevels, this.getType());
  }

  public void create(final Database dbSource, final T item, final IdGenerator idGenerator) throws GroundException {
    final List<String> sqlList = createSqlList(item);
    PostgresUtils.executeSqlList(dbSource, sqlList);
  }

  public List<String> createSqlList(final T item) throws GroundException {
    final List<String> sqlList = new ArrayList<>();
    sqlList.add(
      String.format(
        "insert into item (id) values (%d)",
        item.getId()));
    final Map<String, Tag> tags = item.getTags();
    if (tags != null) {
      for (String key : tags.keySet()) {
        Tag tag = tags.get(key);
        TagDao tagDao = new TagDao();
        sqlList.addAll(tagDao.createSqlList(tag));
      }
    }
    return sqlList;
  }

  public void delete(final Database dbSource, final T item) throws GroundException {
    final List<String> sqlList = new ArrayList<>();
    sqlList.add("begin");
    sqlList.add(String.format("delete from item where id = %d", item.getId()));
    final Map<String, Tag> tags = item.getTags();
    for (String key : tags.keySet()) {
      Tag tag = tags.get(key);

      sqlList.add(String.format("delete from item_tag where item_id = %d", item.getId()));
    }
    sqlList.add("commit");
    PostgresUtils.executeSqlList(dbSource, sqlList);
  }
}
