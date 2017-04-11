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

import edu.berkeley.ground.dao.models.GraphFactory;
import edu.berkeley.ground.dao.versions.neo4j.Neo4jItemFactory;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundDbException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.Graph;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.util.IdGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jGraphFactory extends GraphFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jGraphFactory.class);

  private final Neo4jClient dbClient;
  private final Neo4jItemFactory itemFactory;

  private final IdGenerator idGenerator;

  /**
   * Constructor for the Neo4j graph factory.
   *
   * @param itemFactory the Neo4jItemFactory singleton
   * @param dbClient the Neo4j client
   * @param idGenerator a unique ID generator
   */
  public Neo4jGraphFactory(Neo4jClient dbClient,
                           Neo4jItemFactory itemFactory,
                           IdGenerator idGenerator) {
    this.dbClient = dbClient;
    this.itemFactory = itemFactory;
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
    Graph graph = null;
    try {
      graph = this.retrieveFromDatabase(sourceKey);
    } catch (GroundException e) {
      if (!e.getMessage().contains("No Graph found")) {
        throw e;
      }
    }

    if (graph != null) {
      throw new GroundException("Graph with source_key " + sourceKey + " already exists.");
    }

    long uniqueId = this.idGenerator.generateItemId();

    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("name", GroundType.STRING, name));
    insertions.add(new DbDataContainer("id", GroundType.LONG, uniqueId));
    insertions.add(new DbDataContainer("source_key", GroundType.STRING, sourceKey));

    this.dbClient.addVertex("Graph", insertions);
    this.itemFactory.insertIntoDatabase(uniqueId, tags);

    LOGGER.info("Created graph " + name + ".");

    return GraphFactory.construct(uniqueId, name, sourceKey, tags);
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
    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("source_key", GroundType.STRING, sourceKey));

    Record record;
    try {
      record = this.dbClient.getVertex(predicates);
    } catch (EmptyResultException e) {
      throw new GroundDbException("No Graph found with source_key " + sourceKey + ".");
    }

    long id = record.get("v").asNode().get("id").asLong();
    String name = record.get("v").asNode().get("name").asString();

    Map<String, Tag> tags = this.itemFactory.retrieveFromDatabase(id).getTags();

    LOGGER.info("Retrieved graph " + sourceKey + ".");

    return GraphFactory.construct(id, name, sourceKey, tags);
  }

  @Override
  public void update(long itemId, long childId, List<Long> parentIds) throws GroundException {
    this.itemFactory.update(itemId, childId, parentIds);
  }

  @Override
  public void truncate(long itemId, int numLevels) throws GroundException {
    this.itemFactory.truncate(itemId, numLevels, "graph");
  }
}
