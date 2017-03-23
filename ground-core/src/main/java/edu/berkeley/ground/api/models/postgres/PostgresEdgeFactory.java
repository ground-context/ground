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

package edu.berkeley.ground.api.models.postgres;

import edu.berkeley.ground.api.models.Edge;
import edu.berkeley.ground.api.models.EdgeFactory;
import edu.berkeley.ground.api.models.EdgeVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.api.versions.VersionHistoryDAG;
import edu.berkeley.ground.api.versions.postgres.PostgresItemFactory;
import edu.berkeley.ground.api.versions.postgres.PostgresVersionHistoryDAGFactory;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.db.QueryResults;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PostgresEdgeFactory extends EdgeFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresEdgeFactory.class);
  private final PostgresClient dbClient;
  private final PostgresItemFactory itemFactory;
  private PostgresEdgeVersionFactory edgeVersionFactory;
  private final PostgresVersionHistoryDAGFactory versionHistoryDAGFactory;

  private final IdGenerator idGenerator;

  public PostgresEdgeFactory(PostgresItemFactory itemFactory,
                             PostgresClient dbClient,
                             IdGenerator idGenerator,
                             PostgresVersionHistoryDAGFactory versionHistoryDAGFactory) {
    this.dbClient = dbClient;
    this.itemFactory = itemFactory;
    this.idGenerator = idGenerator;
    this.edgeVersionFactory = null;
    this.versionHistoryDAGFactory = versionHistoryDAGFactory;
  }

  public void setEdgeVersionFactory(PostgresEdgeVersionFactory edgeVersionFactory) {
    this.edgeVersionFactory = edgeVersionFactory;
  }

  public Edge create(String name, long fromNodeId, long toNodeId, Map<String, Tag> tags)
      throws GroundException {
    try {
      long uniqueId = this.idGenerator.generateItemId();

      this.itemFactory.insertIntoDatabase(uniqueId, tags);

      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("name", GroundType.STRING, name));
      insertions.add(new DbDataContainer("item_id", GroundType.LONG, uniqueId));
      insertions.add(new DbDataContainer("from_node_id", GroundType.LONG, fromNodeId));
      insertions.add(new DbDataContainer("to_node_id", GroundType.LONG, toNodeId));

      this.dbClient.insert("edge", insertions);

      this.dbClient.commit();
      LOGGER.info("Created edge " + name + ".");
      return EdgeFactory.construct(uniqueId, name, fromNodeId, toNodeId, tags);
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }
  }

  public Edge retrieveFromDatabase(String name) throws GroundException {
    return this.retrieveByPredicate("name", name, GroundType.STRING);
  }

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
        resultSet = this.dbClient.equalitySelect("edge", DBClient.SELECT_STAR, predicates);
      } catch (EmptyResultException e) {
        throw new GroundException("No Edge found with " + fieldName + " " + value + ".");
      }

      long id = resultSet.getLong(1);
      String name = resultSet.getString(4);
      Map<String, Tag> tags = this.itemFactory.retrieveFromDatabase(id).getTags();
      long fromNodeId = resultSet.getLong(2);
      long toNodeId = resultSet.getLong(3);

      this.dbClient.commit();
      LOGGER.info("Retrieved edge " + value + ".");

      return EdgeFactory.construct(id, name, fromNodeId, toNodeId, tags);
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }
  }

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
        VersionHistoryDAG dag = this.versionHistoryDAGFactory.retrieveFromDatabase(fromNodeId);
        fromEndId = (long) dag.getParent(currentVersion.getFromNodeVersionStartId()).get(0);
      }

      if (parentVersion.getToNodeVersionEndId() == -1) {
        // update to end id
        VersionHistoryDAG dag = this.versionHistoryDAGFactory.retrieveFromDatabase(toNodeId);
        toEndId = (long) dag.getParent(currentVersion.getToNodeVersionStartId()).get(0);
      }

      if (fromEndId != -1 || toEndId != -1) {
        this.edgeVersionFactory.updatePreviousVersion(parentId, fromEndId, toEndId);
      }
    }

  }
}
