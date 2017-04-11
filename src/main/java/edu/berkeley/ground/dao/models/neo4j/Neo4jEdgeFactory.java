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

package edu.berkeley.ground.dao.models.neo4j;

import edu.berkeley.ground.dao.models.EdgeFactory;
import edu.berkeley.ground.dao.versions.neo4j.Neo4jItemFactory;
import edu.berkeley.ground.dao.versions.neo4j.Neo4jVersionHistoryDagFactory;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.exceptions.GroundDbException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.exceptions.GroundVersionNotFoundException;
import edu.berkeley.ground.model.models.Edge;
import edu.berkeley.ground.model.models.EdgeVersion;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.model.versions.VersionHistoryDag;
import edu.berkeley.ground.util.IdGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Neo4jEdgeFactory extends Neo4jItemFactory<Edge> implements EdgeFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jEdgeFactory.class);
  private final Neo4jClient dbClient;
  private final Neo4jVersionHistoryDagFactory versionHistoryDagFactory;
  private  Neo4jEdgeVersionFactory edgeVersionFactory;

  private final IdGenerator idGenerator;

  /**
   * Constructor for Neo4j edge factory.
   *
   * @param dbClient the Neo4j client
   * @param idGenerator a unique ID generator
   * @param versionHistoryDagFactory a Neo4jVersionHistoryDAGFactory singleton
   */
  public Neo4jEdgeFactory(Neo4jClient dbClient,
                          Neo4jVersionHistoryDagFactory versionHistoryDagFactory,
                          Neo4jTagFactory tagFactory,
                          IdGenerator idGenerator) {

    super(dbClient, versionHistoryDagFactory, tagFactory);

    this.dbClient = dbClient;
    this.idGenerator = idGenerator;
    this.edgeVersionFactory = null;
    this.versionHistoryDagFactory = versionHistoryDagFactory;
  }

  public void setEdgeVersionFactory(Neo4jEdgeVersionFactory edgeVersionFactory) {
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

    super.verifyItemNotExists(sourceKey);
    long uniqueId = idGenerator.generateItemId();

    super.insertIntoDatabase(uniqueId, tags);

    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("name", GroundType.STRING, name));
    insertions.add(new DbDataContainer("id", GroundType.LONG, uniqueId));
    insertions.add(new DbDataContainer("from_node_id", GroundType.LONG, fromNodeId));
    insertions.add(new DbDataContainer("to_node_id", GroundType.LONG, toNodeId));
    insertions.add(new DbDataContainer("source_key", GroundType.STRING, sourceKey));

    this.dbClient.addVertex("GroundEdge", insertions);

    LOGGER.info("Created edge " + name + ".");
    return new Edge(uniqueId, name, sourceKey, fromNodeId, toNodeId, tags);
  }

  /**
   * Retrieve the DAG leaves for this edge.
   *
   * @param sourceKey the key of the edge to retrieve leaves for.
   * @return the leaves of the edge
   * @throws GroundException an error while retrieving the edge
   */
  @Override
  public List<Long> getLeaves(String sourceKey) throws GroundException {
    Edge edge = this.retrieveFromDatabase(sourceKey);
    return super.getLeaves(edge.getId());
  }

  @Override
  public Edge retrieveFromDatabase(String sourceKey) throws GroundException {
    return this.retrieveByPredicate("source_key", sourceKey, GroundType.STRING);
  }

  @Override
  public Edge retrieveFromDatabase(long id) throws GroundException {
    return this.retrieveByPredicate("id", id, GroundType.LONG);
  }

  private Edge retrieveByPredicate(String fieldName, Object value, GroundType valueType)
      throws GroundException {
    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer(fieldName, valueType, value));

    Record record = this.dbClient.getVertex(predicates);
    super.verifyResultSet(record, fieldName, value);

    long id = record.get("v").asNode().get("id").asLong();
    long fromNodeId = record.get("v").asNode().get("from_node_id").asLong();
    long toNodeId = record.get("v").asNode().get("to_node_id").asLong();

    String name = record.get("v").asNode().get("name").asString();
    String sourceKey = record.get("v").asNode().get("source_key").asString();

    Map<String, Tag> tags = super.retrieveItemTags(id);


    LOGGER.info("Retrieved edge " + name + ".");

    return new Edge(id, name, sourceKey, fromNodeId, toNodeId, tags);
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
    super.update(itemId, childId, parentIds);

    for (long parentId : parentIds) {
      EdgeVersion currentVersion = this.edgeVersionFactory.retrieveFromDatabase(childId);
      EdgeVersion parentVersion;
      try {
        parentVersion = this.edgeVersionFactory.retrieveFromDatabase(parentId);
      } catch (GroundVersionNotFoundException dbe) {
        return;
      }

      catch (GroundException e) {
        throw e;
      }

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
