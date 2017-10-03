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
package edu.berkeley.ground.cassandra.dao.version;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.CaseFormat;
import edu.berkeley.ground.common.dao.version.ItemDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.exception.GroundException.ExceptionType;
import edu.berkeley.ground.common.model.version.Item;
import edu.berkeley.ground.common.model.version.Tag;
import edu.berkeley.ground.common.model.version.VersionHistoryDag;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.cassandra.dao.CqlConstants;
import edu.berkeley.ground.cassandra.util.CassandraDatabase;
import edu.berkeley.ground.cassandra.util.CassandraStatements;
import edu.berkeley.ground.cassandra.util.CassandraUtils;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import play.libs.Json;


public abstract class CassandraItemDao<T extends Item> implements ItemDao<T> {

  private CassandraVersionHistoryDagDao cassandraVersionHistoryDagDao;
  protected CassandraTagDao cassandraTagDao;
  protected CassandraDatabase dbSource;
  protected IdGenerator idGenerator;

  public CassandraItemDao(CassandraDatabase dbSource, IdGenerator idGenerator) {
    this.dbSource = dbSource;
    this.idGenerator = idGenerator;
    this.cassandraVersionHistoryDagDao = new CassandraVersionHistoryDagDao(dbSource, idGenerator);
    this.cassandraTagDao = new CassandraTagDao(dbSource);
  }

  @Override
  public CassandraStatements insert(final T item) throws GroundException {
    long id = item.getId();

    final List<String> cqlList = new ArrayList<>();
    cqlList.add(String.format(CqlConstants.INSERT_ITEM, id));

    final Map<String, Tag> tags = item.getTags();
    CassandraStatements statements = new CassandraStatements(cqlList);

    if (tags != null) {
      for (String key : tags.keySet()) {
        Tag tag = tags.get(key);
        statements.merge(this.cassandraTagDao.insertItemTag(new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType())));
      }
    }

    return new CassandraStatements(cqlList);
  }

  @Override
  public T retrieveFromDatabase(String sourceKey) throws GroundException {
    return this.retrieve(String.format(CqlConstants.SELECT_STAR_BY_SOURCE_KEY, CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE,
      this.getType().getSimpleName()), sourceKey), sourceKey);
  }

  @Override
  public T retrieveFromDatabase(long id) throws GroundException {
    return this.retrieve(String.format(CqlConstants.SELECT_STAR_ITEM_BY_ID, CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE,
      this.getType().getSimpleName()), id), id);
  }

  @Override
  public List<Long> getLeaves(long itemId) throws GroundException {
    try {
      VersionHistoryDag dag = this.cassandraVersionHistoryDagDao.retrieveFromDatabase(itemId);

      return dag.getLeaves();
    } catch (GroundException e) {
      if (!e.getMessage().contains("No results found for query:")) {
        throw e;
      }

      return new ArrayList<>();
    }
  }

  @Override
  public Map<Long, Long> getHistory(long itemId) throws GroundException {
    try {
      VersionHistoryDag dag = this.cassandraVersionHistoryDagDao.retrieveFromDatabase(itemId);

      return dag.getParentChildPairs();
    } catch (GroundException e) {
      if (!e.getMessage().contains("No results found for query:")) {
        throw e;
      }

      return new HashMap<>();
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
  public CassandraStatements update(long itemId, long childId, List<Long> parentIds) throws GroundException {

    if (parentIds.isEmpty()) {
      parentIds.add(0L);
    }

    VersionHistoryDag dag = this.cassandraVersionHistoryDagDao.retrieveFromDatabase(itemId);
    CassandraStatements statements = new CassandraStatements();

    for (long parentId : parentIds) {
      if (parentId != 0L && !dag.checkItemInDag(parentId)) {
        throw new GroundException(ExceptionType.OTHER, String.format("Parent %d is not in Item %d.", parentId, itemId));
      }

      statements.merge(this.cassandraVersionHistoryDagDao.addEdge(dag, parentId, childId, itemId));
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
    dag = cassandraVersionHistoryDagDao.retrieveFromDatabase(itemId);
    this.cassandraVersionHistoryDagDao.truncate(dag, numLevels, this.getType());
  }

  protected T retrieve(String cql, Object field) throws GroundException {
    JsonNode json = Json.parse(CassandraUtils.executeQueryToJson(dbSource, cql));

    if (json.size() == 0) {
      throw new GroundException(ExceptionType.ITEM_NOT_FOUND, this.getType().getSimpleName(), field.toString());
    }

    Class<T> type = this.getType();
    JsonNode itemJson = json.get(0);
    long id = itemJson.get("itemId").asLong();
    String name = itemJson.get("name").asText();
    String sourceKey = itemJson.get("sourceKey").asText();

    Object[] args = {id, name, sourceKey, this.cassandraTagDao.retrieveFromDatabaseByItemId(id)};

    Constructor<T> constructor;
    try {
      constructor = type.getConstructor(long.class, String.class, String.class, Map.class);
      return constructor.newInstance(args);
    } catch (Exception e) {
      throw new GroundException(ExceptionType.OTHER, String.format("Catastrophic failure: Unable to instantiate Item.\n%s: %s.",
        e.getClass().getName(), e.getMessage()));
    }
  }
}
