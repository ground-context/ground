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

package edu.berkeley.ground.api.models.neo4j;

import edu.berkeley.ground.api.models.Edge;
import edu.berkeley.ground.api.models.EdgeFactory;
import edu.berkeley.ground.api.models.EdgeVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.api.versions.VersionHistoryDAG;
import edu.berkeley.ground.api.versions.neo4j.Neo4jItemFactory;
import edu.berkeley.ground.api.versions.neo4j.Neo4jVersionHistoryDAGFactory;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundDBException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;

import org.neo4j.driver.v1.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Neo4jEdgeFactory extends EdgeFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jEdgeFactory.class);
  private final Neo4jClient dbClient;
  private final Neo4jItemFactory itemFactory;
  private final Neo4jEdgeVersionFactory edgeVersionFactory;
  private final Neo4jVersionHistoryDAGFactory versionHistoryDAGFactory;

  private final IdGenerator idGenerator;

  public Neo4jEdgeFactory(Neo4jItemFactory itemFactory,
                          Neo4jClient dbClient,
                          IdGenerator idGenerator,
                          Neo4jEdgeVersionFactory edgeVersionFactory,
                          Neo4jVersionHistoryDAGFactory versionHistoryDAGFactory) {
    this.dbClient = dbClient;
    this.itemFactory = itemFactory;
    this.idGenerator = idGenerator;
    this.edgeVersionFactory = edgeVersionFactory;
    this.versionHistoryDAGFactory = versionHistoryDAGFactory;
  }

  public Edge create(String name, long fromNodeId, long toNodeId, Map<String, Tag> tags)
      throws GroundException {
    try {
      long uniqueId = idGenerator.generateItemId();

      this.itemFactory.insertIntoDatabase(uniqueId, tags);

      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("name", GroundType.STRING, name));
      insertions.add(new DbDataContainer("id", GroundType.LONG, uniqueId));
      insertions.add(new DbDataContainer("from_node_id", GroundType.LONG, fromNodeId));
      insertions.add(new DbDataContainer("to_node_id", GroundType.LONG, toNodeId));

      this.dbClient.addVertex("GroundEdge", insertions);

      this.dbClient.commit();
      LOGGER.info("Created edge " + name + ".");
      return EdgeFactory.construct(uniqueId, name, fromNodeId, toNodeId, tags);
    } catch (GroundDBException e) {
      this.dbClient.abort();

      throw e;
    }
  }

  public Edge retrieveFromDatabase(String name) throws GroundException {
    return this.retrieveByPredicate("name", name, GroundType.STRING);
  }

  public Edge retrieveFromDatabase(long id) throws GroundException {
    return this.retrieveByPredicate("id", id, GroundType.LONG);
  }

  private Edge retrieveByPredicate(String fieldName, Object value, GroundType valueType)
      throws GroundException {
    try {
      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer(fieldName, valueType, value));

      Record record;
      try {
        record = this.dbClient.getVertex(predicates);
      } catch (EmptyResultException e) {
        throw new GroundDBException("No Edge found with " + fieldName + " " + value + ".");
      }

      long id = record.get("v").asNode().get("id").asLong();
      String name = record.get("v").asNode().get("name").asString();
      Map<String, Tag> tags = this.itemFactory.retrieveFromDatabase(id).getTags();
      long fromNodeId = record.get("v").asNode().get("from_node_id").asLong();
      long toNodeId = record.get("v").asNode().get("to_node_id").asLong();


      this.dbClient.commit();
      LOGGER.info("Retrieved edge " + name + ".");

      return EdgeFactory.construct(id, name, fromNodeId, toNodeId, tags);
    } catch (GroundDBException e) {
      this.dbClient.abort();

      throw e;
    }
  }


  public void update(long itemId, long childId, List<Long> parentIds) throws GroundException {
    this.itemFactory.update(itemId, childId, parentIds);


    for (long parentId : parentIds) {
      EdgeVersion currentVersion = this.edgeVersionFactory.retrieveFromDatabase(childId);
      EdgeVersion parentVersion = this.edgeVersionFactory.retrieveFromDatabase(parentId);
      Edge edge = this.retrieveFromDatabase(itemId);

      long fromNodeId = edge.getFromNodeId();
      long toNodeId = edge.getToNodeId();

      long fromEndId = -1;
      long toEndId = -1;

      if (parentVersion.getFromNodeVersionEndId() != -1) {
        // update from end id
        VersionHistoryDAG dag = this.versionHistoryDAGFactory.retrieveFromDatabase(fromNodeId);
        fromEndId = (long) dag.getParent(currentVersion.getFromNodeVersionStartId()).get(0);
      }

      if (parentVersion.getToNodeVersionEndId() != -1) {
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
