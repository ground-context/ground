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

package edu.berkeley.ground.dao.models.cassandra;

import edu.berkeley.ground.dao.models.GraphFactory;
import edu.berkeley.ground.dao.versions.cassandra.CassandraItemFactory;
import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.db.CassandraResults;
import edu.berkeley.ground.db.DbClient;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.Graph;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.util.IdGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CassandraGraphFactory extends GraphFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraGraphFactory.class);
  private final CassandraClient dbClient;
  private final CassandraItemFactory itemFactory;

  private final IdGenerator idGenerator;

  /**
   * Constructor for the Cassandra graph factory.
   *
   * @param itemFactory the CassandraItemFactory singleton
   * @param dbClient the Cassandra client
   * @param idGenerator a unique ID generator
   */
  public CassandraGraphFactory(CassandraItemFactory itemFactory,
                               CassandraClient dbClient,
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
    long uniqueId = this.idGenerator.generateItemId();

    this.itemFactory.insertIntoDatabase(uniqueId, tags);

    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("name", GroundType.STRING, name));
    insertions.add(new DbDataContainer("item_id", GroundType.LONG, uniqueId));
    insertions.add(new DbDataContainer("source_key", GroundType.STRING, sourceKey));

    this.dbClient.insert("graph", insertions);

    LOGGER.info("Created graph " + name + ".");

    return GraphFactory.construct(uniqueId, name, sourceKey, tags);
  }

  /**
   * Retrieves an edge from the database.
   *
   * @param sourceKey the key of the graph to retrieve
   * @return the retrieved graph
   * @throws GroundException either the graph doesn't exist or couldn't be retrieved
   */
  @Override
  public Graph retrieveFromDatabase(String sourceKey) throws GroundException {
    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("source_key", GroundType.STRING, sourceKey));

    CassandraResults resultSet;
    try {
      resultSet = this.dbClient.equalitySelect("graph", DbClient.SELECT_STAR, predicates);
    } catch (EmptyResultException e) {
      throw new GroundException("No Graph found with source_key " + sourceKey + ".");
    }

    long id = resultSet.getLong("item_id");
    String name = resultSet.getString("name");

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
