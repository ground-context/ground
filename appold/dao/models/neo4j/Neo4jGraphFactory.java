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

package dao.models.neo4j;

import dao.models.GraphFactory;
import dao.versions.neo4j.Neo4jItemFactory;
import dao.versions.neo4j.Neo4jVersionHistoryDagFactory;
import db.DbDataContainer;
import db.Neo4jClient;
import edu.berkeley.ground.exception.GroundException;
import models.models.Graph;
import edu.berkeley.ground.model.version.Tag;
import models.versions.GroundType;
import util.IdGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jGraphFactory extends Neo4jItemFactory<Graph> implements GraphFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jGraphFactory.class);

  private final Neo4jClient dbClient;
  private final IdGenerator idGenerator;

  /**
   * Constructor for the Neo4j graph factory.
   *
   * @param dbClient the Neo4j client
   * @param idGenerator a unique ID generator
   */
  public Neo4jGraphFactory(Neo4jClient dbClient,
                           Neo4jVersionHistoryDagFactory versionHistoryDagFactory,
                           Neo4jTagFactory tagFactory,
                           IdGenerator idGenerator) {

    super(dbClient, versionHistoryDagFactory, tagFactory);

    this.dbClient = dbClient;
    this.idGenerator = idGenerator;
  }

  /**
   * Creates and persists a graph.
   *
   * @param name the name of the graph
   * @param sourceKey the user generated unique key for the graph
   * @param tags tags associated with this graph
   * @return the created graph
   * @throws GroundException an error while persisting the graph
   */
  @Override
  public Graph create(String name, String sourceKey, Map<String, Tag> tags) throws GroundException {
    super.verifyItemNotExists(sourceKey);

    long uniqueId = this.idGenerator.generateItemId();

    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("name", GroundType.STRING, name));
    insertions.add(new DbDataContainer("id", GroundType.LONG, uniqueId));
    insertions.add(new DbDataContainer("source_key", GroundType.STRING, sourceKey));

    this.dbClient.addVertex("Graph", insertions);
    super.insertIntoDatabase(uniqueId, tags);

    LOGGER.info("Created graph " + name + ".");

    return new Graph(uniqueId, name, sourceKey, tags);
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
    Graph graph = this.retrieveFromDatabase(sourceKey);
    return super.getLeaves(graph.getId());
  }

  /**
   * Retrieves a graph from the database.
   *
   * @param sourceKey the key of the graph to retrieve
   * @return the retrieved graph
   * @throws GroundException either the graph doesn't exist or couldn't be retrieved
   */
  @Override
  public Graph retrieveFromDatabase(String sourceKey) throws GroundException {
    return this.retrieveByPredicate("source_key", sourceKey, GroundType.STRING);
  }

  /**
   * Retrieves a graph from the database.
   *
   * @param id the id of the graph to retrieve
   * @return the retrieved graph
   * @throws GroundException either the graph doesn't exist or couldn't be retrieved
   */
  @Override
  public Graph retrieveFromDatabase(long id) throws GroundException {
    return this.retrieveByPredicate("id", id, GroundType.LONG);
  }

  private Graph retrieveByPredicate(String fieldName, Object value, GroundType valueType)
      throws GroundException {

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer(fieldName, valueType, value));

    Record record = this.dbClient.getVertex(predicates);
    super.verifyResultSet(record, fieldName, value);

    long id = record.get("v").asNode().get("id").asLong();
    String name = record.get("v").asNode().get("name").asString();
    String sourceKey = record.get("v").asNode().get("source_key").asString();

    Map<String, Tag> tags = super.retrieveItemTags(id);

    LOGGER.info("Retrieved graph " + value + ".");

    return new Graph(id, name, sourceKey, tags);
  }
}
