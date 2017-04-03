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

import edu.berkeley.ground.dao.models.GraphFactory;
import edu.berkeley.ground.dao.versions.postgres.PostgresItemFactory;
import edu.berkeley.ground.db.DbClient;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.db.QueryResults;
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

public class PostgresGraphFactory extends GraphFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresGraphFactory.class);
  private final PostgresClient dbClient;
  private final PostgresItemFactory itemFactory;

  private final IdGenerator idGenerator;

  /**
   * Constructor for the Postgres graph factory.
   *
   * @param itemFactory the PostgresItemFactory singleton
   * @param dbClient the Postgres client
   * @param idGenerator a unique ID generator
   */
  public PostgresGraphFactory(PostgresItemFactory itemFactory,
                              PostgresClient dbClient,
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
    try {
      long uniqueId = this.idGenerator.generateItemId();

      this.itemFactory.insertIntoDatabase(uniqueId, tags);

      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("name", GroundType.STRING, name));
      insertions.add(new DbDataContainer("item_id", GroundType.LONG, uniqueId));
      insertions.add(new DbDataContainer("source_key", GroundType.STRING, sourceKey));

      this.dbClient.insert("graph", insertions);

      this.dbClient.commit();
      LOGGER.info("Created graph " + name + ".");

      return GraphFactory.construct(uniqueId, name, sourceKey, tags);
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }
  }

  /**
   * Retrieves an edge from the database.
   *
   * @param name the name of the graph to retrieve
   * @return the retrieved graph
   * @throws GroundException either the graph doesn't exist or couldn't be retrieved
   */
  @Override
  public Graph retrieveFromDatabase(String name) throws GroundException {
    try {
      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer("name", GroundType.STRING, name));

      QueryResults resultSet;
      try {
        resultSet = this.dbClient.equalitySelect("graph", DbClient.SELECT_STAR, predicates);
      } catch (EmptyResultException e) {
        throw new GroundException("No Graph found with name " + name + ".");
      }

      long id = resultSet.getLong(1);
      String sourceKey = resultSet.getString(2);

      Map<String, Tag> tags = this.itemFactory.retrieveFromDatabase(id).getTags();

      this.dbClient.commit();
      LOGGER.info("Retrieved graph " + name + ".");

      return GraphFactory.construct(id, name, sourceKey, tags);
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
    this.itemFactory.truncate(itemId, numLevels, "edge");
  }
}
