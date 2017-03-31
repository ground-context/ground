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

package edu.berkeley.ground.dao.models.postgres;

import edu.berkeley.ground.dao.models.EdgeFactory;
import edu.berkeley.ground.dao.versions.postgres.PostgresItemFactory;
import edu.berkeley.ground.dao.versions.postgres.PostgresVersionHistoryDagFactory;
import edu.berkeley.ground.db.DbClient;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.db.QueryResults;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.Edge;
import edu.berkeley.ground.model.models.EdgeVersion;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.model.versions.VersionHistoryDag;
import edu.berkeley.ground.util.IdGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresEdgeFactory extends EdgeFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresEdgeFactory.class);
  private final PostgresClient dbClient;
  private final PostgresItemFactory itemFactory;
  private PostgresEdgeVersionFactory edgeVersionFactory;
  private final PostgresVersionHistoryDagFactory versionHistoryDagFactory;

  private final IdGenerator idGenerator;

  /**
   * Constructor for Postgres edge factory.
   *
   * @param itemFactory a PostgresItemFactory singleton
   * @param dbClient the Postgres client
   * @param idGenerator a unique ID generator
   * @param versionHistoryDagFactory a PostgresVersionHistoryDAGFactory singleton
   */
  public PostgresEdgeFactory(PostgresItemFactory itemFactory,
                             PostgresClient dbClient,
                             IdGenerator idGenerator,
                             PostgresVersionHistoryDagFactory versionHistoryDagFactory) {
    this.dbClient = dbClient;
    this.itemFactory = itemFactory;
    this.idGenerator = idGenerator;
    this.edgeVersionFactory = null;
    this.versionHistoryDagFactory = versionHistoryDagFactory;
  }

  public void setEdgeVersionFactory(PostgresEdgeVersionFactory edgeVersionFactory) {
    this.edgeVersionFactory = edgeVersionFactory;
  }


  /**
   * Creates and persists a new edge.
   *
   * @param name the name of the edge
   * @param sourceKey the user generated unique key for the edge
   * @param fromNodeId the id of the originating node for this edg
   * @param toNodeId the id of the destination node for this edg
   * @param tags tags on this edge
   * @return the created edge
   * @throws GroundException an error while creating or persisting the edge
   */
  @Override
  public Edge create(String name,
                     String sourceKey,
                     long fromNodeId,
                     long toNodeId,
                     Map<String, Tag> tags) throws GroundException {
    try {
      long uniqueId = this.idGenerator.generateItemId();

      this.itemFactory.insertIntoDatabase(uniqueId, tags);

      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("name", GroundType.STRING, name));
      insertions.add(new DbDataContainer("item_id", GroundType.LONG, uniqueId));
      insertions.add(new DbDataContainer("from_node_id", GroundType.LONG, fromNodeId));
      insertions.add(new DbDataContainer("to_node_id", GroundType.LONG, toNodeId));
      insertions.add(new DbDataContainer("source_key", GroundType.STRING, sourceKey));

      this.dbClient.insert("edge", insertions);

      this.dbClient.commit();
      LOGGER.info("Created edge " + name + ".");
      return EdgeFactory.construct(uniqueId, name, sourceKey, fromNodeId, toNodeId, tags);
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }
  }

  @Override
  public Edge retrieveFromDatabase(String name) throws GroundException {
    return this.retrieveByPredicate("name", name, GroundType.STRING);
  }

  @Override
  public Edge retrieveFromDatabase(long id) throws GroundException {
    return this.retrieveByPredicate("item_id", id, GroundType.LONG);
  }

  private Edge retrieveByPredicate(String fieldName, Object value, GroundType valueType)
      throws GroundException {
    try {
      List<DbDataContainer> predicates = new ArrayList<>();

      predicates.add(new DbDataContainer(fieldName, valueType, value));

      QueryResults resultSet;
      try {
        resultSet = this.dbClient.equalitySelect("edge", DbClient.SELECT_STAR, predicates);
      } catch (EmptyResultException e) {
        throw new GroundException("No Edge found with " + fieldName + " " + value + ".");
      }

      long id = resultSet.getLong(1);
      long fromNodeId = resultSet.getLong(3);
      long toNodeId = resultSet.getLong(4);

      String name = resultSet.getString(5);
      String sourceKey = resultSet.getString(2);

      Map<String, Tag> tags = this.itemFactory.retrieveFromDatabase(id).getTags();

      this.dbClient.commit();
      LOGGER.info("Retrieved edge " + value + ".");

      return EdgeFactory.construct(id, name, sourceKey, fromNodeId, toNodeId, tags);
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }
  }

  /**
   * Update this edge with a new version.
   *
   * @param itemId the item id of the edge
   * @param childId the id of the new child
   * @param parentIds the ids of any parents of the child
   * @throws GroundException an unexpected error during the update
   */
  @Override
  public void update(long itemId, long childId, List<Long> parentIds) throws GroundException {
    this.itemFactory.update(itemId, childId, parentIds);
    parentIds = parentIds.stream().filter(x -> x != 0).collect(Collectors.toList());

    for (long parentId : parentIds) {
      EdgeVersion currentVersion = this.edgeVersionFactory.retrieveFromDatabase(childId);
      EdgeVersion parentVersion = this.edgeVersionFactory.retrieveFromDatabase(parentId);
      Edge edge = this.retrieveFromDatabase(itemId);

      long fromNodeId = edge.getFromNodeId();
      long toNodeId = edge.getToNodeId();

      long fromEndId = -1;
      long toEndId = -1;

      if (parentVersion.getFromNodeVersionEndId() == -1) {
        // update from end id
        VersionHistoryDag dag = this.versionHistoryDagFactory.retrieveFromDatabase(fromNodeId);
        fromEndId = (long) dag.getParent(currentVersion.getFromNodeVersionStartId()).get(0);
      }

      if (parentVersion.getToNodeVersionEndId() == -1) {
        // update to end id
        VersionHistoryDag dag = this.versionHistoryDagFactory.retrieveFromDatabase(toNodeId);
        toEndId = (long) dag.getParent(currentVersion.getToNodeVersionStartId()).get(0);
      }

      if (fromEndId != -1 || toEndId != -1) {
        this.edgeVersionFactory.updatePreviousVersion(parentId, fromEndId, toEndId);
      }
    }

  }
}
