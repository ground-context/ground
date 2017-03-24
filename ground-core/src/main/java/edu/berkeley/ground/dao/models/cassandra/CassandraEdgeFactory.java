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

package edu.berkeley.ground.dao.models.cassandra;

import edu.berkeley.ground.model.models.Edge;
import edu.berkeley.ground.dao.models.EdgeFactory;
import edu.berkeley.ground.model.models.EdgeVersion;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.model.versions.VersionHistoryDAG;
import edu.berkeley.ground.dao.versions.cassandra.CassandraItemFactory;
import edu.berkeley.ground.dao.versions.cassandra.CassandraVersionHistoryDAGFactory;
import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DbDataContainer;
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

public class CassandraEdgeFactory extends EdgeFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraEdgeFactory.class);
  private final CassandraClient dbClient;
  private final CassandraVersionHistoryDAGFactory versionHistoryDAGFactory;
  private final CassandraItemFactory itemFactory;
  private CassandraEdgeVersionFactory edgeVersionFactory;

  private final IdGenerator idGenerator;

  public CassandraEdgeFactory(CassandraItemFactory itemFactory,
                              CassandraClient dbClient,
                              IdGenerator idGenerator,
                              CassandraVersionHistoryDAGFactory versionHistoryDAGFactory) {
    this.dbClient = dbClient;
    this.itemFactory = itemFactory;
    this.idGenerator = idGenerator;
    this.edgeVersionFactory = null;
    this.versionHistoryDAGFactory = versionHistoryDAGFactory;
  }

  public void setEdgeVersionFactory(CassandraEdgeVersionFactory edgeVersionFactory) {
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
      throws GroundException{

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer(fieldName, valueType, value));

    try {
      QueryResults resultSet;
      try {
        resultSet = this.dbClient.equalitySelect("edge", DBClient.SELECT_STAR, predicates);
      } catch (EmptyResultException e) {
        this.dbClient.abort();

        throw new GroundException("No Edge found with " + fieldName + " " + value + ".");
      }

      if (!resultSet.next()) {
        this.dbClient.abort();

        throw new GroundException("No Edge found with " + fieldName + " " + value + ".");
      }

      long id = resultSet.getLong(0);
      String name = resultSet.getString("name");
      Map<String, Tag> tags = this.itemFactory.retrieveFromDatabase(id).getTags();
      long fromNodeId = resultSet.getLong("from_node_id");
      long toNodeId = resultSet.getLong("to_node_id");

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
