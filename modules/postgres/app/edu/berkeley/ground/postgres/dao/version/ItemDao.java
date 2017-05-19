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

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.factory.version.ItemFactory;
import edu.berkeley.ground.common.factory.version.TagFactory;
import edu.berkeley.ground.common.model.core.Node;
import edu.berkeley.ground.common.model.version.GroundType;
import edu.berkeley.ground.common.model.version.Item;
import edu.berkeley.ground.common.model.version.Tag;
import edu.berkeley.ground.common.model.version.VersionHistoryDag;
import edu.berkeley.ground.common.utils.IdGenerator;
import edu.berkeley.ground.postgres.utils.PostgresStatements;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import javafx.geometry.Pos;
import play.db.Database;
import play.libs.Json;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ItemDao<T extends Item> implements ItemFactory<T> {
  protected VersionHistoryDagDao versionHistoryDagDao;
  protected TagFactory tagFactory;
  protected Database dbSource;
  protected IdGenerator idGenerator;

  public ItemDao(Database dbSource, IdGenerator idGenerator) {
    this.dbSource = dbSource;
    this.idGenerator = idGenerator;
  }

  public ItemDao(Database dbSource, IdGenerator idGenerator, VersionHistoryDagDao
    versionHistoryDagDao, TagFactory tagFactory) {
    this.dbSource = dbSource;
    this.idGenerator = idGenerator;
    this.versionHistoryDagDao = versionHistoryDagDao;
    this.tagFactory = tagFactory;
  }

  public Item getItemData(long id) throws GroundException {
    String sql =
      String.format("select * from item where id=%d", id);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    if (json.size() == 0) {
      throw new GroundException(String.format("Itemd with id %d does not exist.", id));
    }
    json = json.get(0);
    Item item = Json.fromJson(json, Item.class);
    sql = String.format("select * from item_tag where item_id=%d", item.getId());
    JsonNode tagsJson = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    HashMap<String, Tag> tags = new HashMap<>();
    for (JsonNode tag : tagsJson) {
      GroundType type = GroundType.fromString(tag.get("type").asText());
      // TODO value isn't always text...
      tags.put(tag.get("key").asText(), new Tag(tag.get("itemId").asLong(), tag.get("key").asText(),
        tag.get("value").asText(), type));
    }
    item = new Item(item.getId(), tags);
    return item;
  }

  public T retrieveFromDatabase(long id) throws GroundException {
    return null;
  }

  @Override
  public T retrieveFromDatabase(String sourceKey) throws GroundException {
    return null;
  }

  @Override
  public Class<T> getType() {
    return null;
  }

  @Override
  public List<Long> getLeaves(long itemId) throws GroundException {
    if (this.versionHistoryDagDao == null) {
      this.versionHistoryDagDao = new VersionHistoryDagDao(dbSource, new VersionSuccessorDao(dbSource, idGenerator));
    }
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
  //Should return sqlList and also add edges to the versionHistoryDag
  @Override
  public PostgresStatements update(long itemId, long childId, List<Long> parentIds) throws GroundException {
    // If a parent is specified, great. If it's not specified, then make it a child of EMPTY.
    if (parentIds.isEmpty()) {
      parentIds.add(0L);
    }

    VersionHistoryDag dag;
    PostgresStatements statements = new PostgresStatements();
    try {
      dag = this.versionHistoryDagDao.retrieveFromDatabase(itemId);
    } catch (GroundException e) {
      if (!e.getMessage().contains("No results found for query:")) {
        throw e;
      }

      dag = this.versionHistoryDagDao.create(itemId);
    }

    for (long parentId : parentIds) {
      if (parentId != 0L && !dag.checkItemInDag(parentId)) {
        String errorString = "Parent " + parentId + " is not in Item " + itemId + ".";
        throw new GroundException(errorString);
      }
      statements.merge(versionHistoryDagDao.insert(dag, parentId, childId, itemId));
      this.versionHistoryDagDao.addEdge(dag, parentId, childId, itemId);
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
    VersionHistoryDag<?> dag;
    if (versionHistoryDagDao == null) {
      versionHistoryDagDao = new VersionHistoryDagDao(dbSource, new VersionSuccessorDao(dbSource, idGenerator));
    }
    dag = versionHistoryDagDao.retrieveFromDatabase(itemId);
    //TODO versionHistoryDagDao is null (not passed in)
    this.versionHistoryDagDao.truncate(dag, numLevels, this.getType());
  }

  @Override
  public PostgresStatements insert(final T item) throws GroundException {
    final List<String> sqlList = new ArrayList<>();
    sqlList.add(
      String.format(
        "insert into item (id) values (%d)",
        item.getId()));
    final Map<String, Tag> tags = item.getTags();
    PostgresStatements postgresStatements = new PostgresStatements(sqlList);
    if (tags != null) {
      for (String key : tags.keySet()) {
        Tag tag = tags.get(key);
        TagDao tagDao = new TagDao(dbSource, idGenerator);
        postgresStatements.merge(tagDao.insert(tag));
      }
    }
    return new PostgresStatements(sqlList);
  }

  public void delete(final T item) throws GroundException {
    PostgresStatements statements = new PostgresStatements();
    statements.append("begin");
    statements.append(String.format("delete from item where id = %d", item.getId()));
    final Map<String, Tag> tags = item.getTags();
    for (String key : tags.keySet()) {
      Tag tag = tags.get(key);
      statements.append(String.format("delete from item_tag where item_id = %d", item.getId()));
    }
    statements.append("commit");
    PostgresUtils.executeSqlList(dbSource, statements);
  }
}
