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
package edu.berkeley.ground.postgres.dao.version;

import edu.berkeley.ground.common.dao.version.ItemDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.version.Item;
import edu.berkeley.ground.common.model.version.Tag;
import edu.berkeley.ground.common.model.version.VersionHistoryDag;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.util.PostgresStatements;
import edu.berkeley.ground.postgres.util.PostgresUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import play.db.Database;

public abstract class PostgresItemDao<T extends Item> implements ItemDao<T> {

  private PostgresTagDao postgresTagDao;
  private PostgresVersionHistoryDagDao postgresVersionHistoryDagDao;
  protected Database dbSource;
  protected IdGenerator idGenerator;

  public PostgresItemDao(Database dbSource, IdGenerator idGenerator) {
    this.dbSource = dbSource;
    this.idGenerator = idGenerator;
    this.postgresVersionHistoryDagDao = new PostgresVersionHistoryDagDao(dbSource, new PostgresVersionSuccessorDao(dbSource, idGenerator));
    this.postgresTagDao = new PostgresTagDao(dbSource);
  }

  @Override
  public List<Long> getLeaves(long itemId) throws GroundException {
    if (this.postgresVersionHistoryDagDao == null) {
      this.postgresVersionHistoryDagDao = new PostgresVersionHistoryDagDao(dbSource, new PostgresVersionSuccessorDao(dbSource, idGenerator));
    }
    try {
      VersionHistoryDag dag = this.postgresVersionHistoryDagDao.retrieveFromDatabase(itemId);

      return dag.getLeaves();
    } catch (GroundException e) {
      if (!e.getMessage().contains("No results found for query:")) {
        throw e;
      }

      return new ArrayList<>();
    }
  }

  @Override
  public T create(T Item) throws GroundException {
    PostgresStatements statements = insert(Item);
    PostgresUtils.executeSqlList(dbSource, statements);
    return Item;
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
  public PostgresStatements update(long itemId, long childId, List<Long> parentIds)
    throws GroundException {
    if (parentIds.isEmpty()) {
      parentIds.add(0L);
    }

    VersionHistoryDag dag;
    PostgresStatements statements = new PostgresStatements();
    try {
      dag = this.postgresVersionHistoryDagDao.retrieveFromDatabase(itemId);
    } catch (GroundException e) {
      if (!e.getMessage().contains("No results found for query:")) {
        throw e;
      }

      dag = this.postgresVersionHistoryDagDao.create(itemId);
    }

    for (long parentId : parentIds) {
      if (parentId != 0L && !dag.checkItemInDag(parentId)) {
        String errorString = "Parent " + parentId + " is not in Item " + itemId + ".";
        throw new GroundException(errorString);
      }

      statements.merge(this.postgresVersionHistoryDagDao.addEdge(dag, parentId, childId, itemId));
    }

    return statements;
  }

  /**
   * Truncate the item to only have the most recent levels.
   *
   * @param numLevels the levels to keep
   * @throws GroundException an error while removing versions
   */
  @Override
  public void truncate(long itemId, int numLevels) throws GroundException {
    VersionHistoryDag dag;
    if (postgresVersionHistoryDagDao == null) {
      postgresVersionHistoryDagDao = new PostgresVersionHistoryDagDao(dbSource, new PostgresVersionSuccessorDao(dbSource, idGenerator));
    }

    dag = postgresVersionHistoryDagDao.retrieveFromDatabase(itemId);
    this.postgresVersionHistoryDagDao.truncate(dag, numLevels, this.getType());
  }

  @Override
  public PostgresStatements insert(final T item) throws GroundException {
    final List<String> sqlList = new ArrayList<>();
    sqlList.add(String.format("insert into item (id) values (%d)", item.getId()));

    final Map<String, Tag> tags = item.getTags();
    PostgresStatements postgresStatements = new PostgresStatements(sqlList);

    if (tags != null) {
      for (String key : tags.keySet()) {
        Tag tag = tags.get(key);
        tag.setId(item.getId());
        postgresStatements.merge(this.postgresTagDao.insert(tag));
      }
    }
    return new PostgresStatements(sqlList);
  }
}
