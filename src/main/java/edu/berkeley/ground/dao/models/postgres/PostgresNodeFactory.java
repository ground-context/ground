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

import edu.berkeley.ground.dao.models.NodeFactory;
import edu.berkeley.ground.dao.versions.postgres.PostgresItemFactory;
import edu.berkeley.ground.db.DbClient;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.db.PostgresResults;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.Node;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.util.IdGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresNodeFactory extends NodeFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresNodeFactory.class);
  private final PostgresClient dbClient;
  private final PostgresItemFactory itemFactory;

  private final IdGenerator idGenerator;

  /**
   * Constructor for the Postgres node factory.
   *
   * @param itemFactory the singleton PostgresItemFactory
   * @param dbClient the Postgres client
   * @param idGenerator a unique id generator
   */
  public PostgresNodeFactory(PostgresItemFactory itemFactory,
                             PostgresClient dbClient,
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
    try {
      long uniqueId = this.idGenerator.generateItemId();

      this.itemFactory.insertIntoDatabase(uniqueId, tags);

      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("name", GroundType.STRING, name));
      insertions.add(new DbDataContainer("item_id", GroundType.LONG, uniqueId));
      insertions.add(new DbDataContainer("source_key", GroundType.STRING, sourceKey));

      this.dbClient.insert("node", insertions);

      this.dbClient.commit();
      LOGGER.info("Created node " + name + ".");

      return NodeFactory.construct(uniqueId, name, sourceKey, tags);
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }
  }

  /**
   * Retrieve the DAG leaves for this node.
   *
   * @param name the name of the node to retrieve leaves for.
   * @return the leaves of the node
   * @throws GroundException an error while retrieving the node
   */
  @Override
  public List<Long> getLeaves(String name) throws GroundException {
    Node node = this.retrieveFromDatabase(name);

    List<Long> leaves = this.itemFactory.getLeaves(node.getId());
    this.dbClient.commit();

    return leaves;
  }


  /**
   * Retrieve a node from the database.
   *
   * @param name the name of the node to retrieve
   * @return the retrieved node
   * @throws GroundException either the node doesn't exist or couldn't be retrieved
   */
  @Override
  public Node retrieveFromDatabase(String name) throws GroundException {
    try {
      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer("name", GroundType.STRING, name));

      PostgresResults resultSet;
      try {
        resultSet = this.dbClient.equalitySelect("node", DbClient.SELECT_STAR, predicates);
      } catch (EmptyResultException e) {
        throw new GroundException("No Node found with name " + name + ".");
      }

      long id = resultSet.getLong(1);
      String sourceKey = resultSet.getString(2);

      Map<String, Tag> tags = this.itemFactory.retrieveFromDatabase(id).getTags();

      this.dbClient.commit();
      LOGGER.info("Retrieved node " + name + ".");

      return NodeFactory.construct(id, name, sourceKey, tags);
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }
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
