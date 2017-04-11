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
import edu.berkeley.ground.dao.versions.neo4j.Neo4jVersionHistoryDagFactory;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
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

public class Neo4jNodeFactory extends Neo4jItemFactory<Node> implements NodeFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jNodeFactory.class);
  private final Neo4jClient dbClient;

  private final IdGenerator idGenerator;

  /**
   * Constructor for the Neo4j node factory.
   *
   * @param dbClient the Neo4j client
   * @param idGenerator a unique id generator
   */
  public Neo4jNodeFactory(Neo4jClient dbClient,
                          Neo4jVersionHistoryDagFactory versionHistoryDagFactory,
                          Neo4jTagFactory tagFactory,
                          IdGenerator idGenerator) {

    super(dbClient, versionHistoryDagFactory, tagFactory);

    this.dbClient = dbClient;
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

    super.verifyItemNotExists(sourceKey);
    long uniqueId = this.idGenerator.generateItemId();

    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("name", GroundType.STRING, name));
    insertions.add(new DbDataContainer("id", GroundType.LONG, uniqueId));
    insertions.add(new DbDataContainer("source_key", GroundType.STRING, sourceKey));

    this.dbClient.addVertex("Node", insertions);
    super.insertIntoDatabase(uniqueId, tags);

    LOGGER.info("Created node " + name + ".");

    return new Node(uniqueId, name, sourceKey, tags);
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
    return super.getLeaves(node.getId());
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
    return this.retrieveByPredicate("source_key", sourceKey, GroundType.STRING);
  }

  /**
   * Retrieve a node from the database.
   *
   * @param id the key of the node to retrieve
   * @return the retrieved node
   * @throws GroundException either the node doesn't exist or couldn't be retrieved
   */
  @Override
  public Node retrieveFromDatabase(long id) throws GroundException {
    return this.retrieveByPredicate("id", id, GroundType.STRING);
  }

  private Node retrieveByPredicate(String fieldName, Object value, GroundType valueType)
      throws GroundException {

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer(fieldName, valueType, value));

    Record record = this.dbClient.getVertex("Node", predicates);
    super.verifyResultSet(record, fieldName, value);

    long id = record.get("v").asNode().get("id").asLong();
    String name = record.get("v").asNode().get("name").asString();
    String sourceKey = record.get("v").asNode().get("source_key").asString();

    Map<String, Tag> tags = super.retrieveItemTags(id);

    LOGGER.info("Retrieved node " + value + ".");

    return new Node(id, name, sourceKey, tags);
  }
}
