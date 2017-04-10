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

import edu.berkeley.ground.dao.models.NodeFactory;
import edu.berkeley.ground.dao.versions.neo4j.Neo4jItemFactory;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundDbException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.Node;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.util.IdGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jNodeFactory extends NodeFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jNodeFactory.class);
  private final Neo4jClient dbClient;
  private final Neo4jItemFactory itemFactory;

  private final IdGenerator idGenerator;

  /**
   * Constructor for the Neo4j node factory.
   *
   * @param itemFactory the singleton Neo4jItemFactory
   * @param dbClient the Neo4j client
   * @param idGenerator a unique id generator
   */
  public Neo4jNodeFactory(Neo4jItemFactory itemFactory,
                          Neo4jClient dbClient,
                          IdGenerator idGenerator) {
    this.dbClient = dbClient;
    this.itemFactory = itemFactory;
    this.idGenerator = idGenerator;
  }

  /**
   * Create and persist a node.
   *
   * @param name the name of the node to create
   * @param sourceKey the user generated unique id for the node
   * @param tags tags associated with this node
   * @return the newly created node
   * @throws GroundException an error while creating or persisting the node
   */
  @Override
  public Node create(String name, String sourceKey, Map<String, Tag> tags) throws GroundException {
    Node node = null;
    try {
      node = this.retrieveFromDatabase(sourceKey);
    } catch (GroundException e) {
      if (!e.getMessage().contains("No Node found")) {
        throw e;
      }
    }

    if (node != null) {
      throw new GroundException("Node with source_key " + sourceKey + " already exists.");
    }

    long uniqueId = this.idGenerator.generateItemId();

    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("name", GroundType.STRING, name));
    insertions.add(new DbDataContainer("id", GroundType.LONG, uniqueId));
    insertions.add(new DbDataContainer("source_key", GroundType.STRING, sourceKey));

    this.dbClient.addVertex("Node", insertions);
    this.itemFactory.insertIntoDatabase(uniqueId, tags);

    LOGGER.info("Created node " + name + ".");

    return NodeFactory.construct(uniqueId, name, sourceKey, tags);
  }

  /**
   * Retrieve the DAG leaves for this node.
   *
   * @param sourceKey the key of the node to retrieve leaves for.
   * @return the leaves of the node
   * @throws GroundException an error while retrieving the node
   */
  @Override
  public List<Long> getLeaves(String sourceKey) throws GroundException {
    Node node = this.retrieveFromDatabase(sourceKey);
    List<Long> leaves = this.itemFactory.getLeaves(node.getId());

    return leaves;
  }

  /**
   * Retrieve a node from the database.
   *
   * @param sourceKey the key of the node to retrieve
   * @return the retrieved node
   * @throws GroundException either the node doesn't exist or couldn't be retrieved
   */
  @Override
  public Node retrieveFromDatabase(String sourceKey) throws GroundException {
    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("source_key", GroundType.STRING, sourceKey));

    Record record;
    try {
      record = this.dbClient.getVertex("Node", predicates);
    } catch (EmptyResultException e) {
      throw new GroundDbException("No Node found with source_key " + sourceKey + ".");
    }

    long id = record.get("v").asNode().get("id").asLong();
    String name = record.get("v").asNode().get("name").asString();

    Map<String, Tag> tags = this.itemFactory.retrieveFromDatabase(id).getTags();

    LOGGER.info("Retrieved node " + sourceKey + ".");

    return NodeFactory.construct(id, name, sourceKey, tags);
  }

  @Override
  public void update(long itemId, long childId, List<Long> parentIds) throws GroundException {
    this.itemFactory.update(itemId, childId, parentIds);
  }

  @Override
  public void truncate(long itemId, int numLevels) throws GroundException {
    this.itemFactory.truncate(itemId, numLevels, "node");
  }
}
