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

package edu.berkeley.ground.dao.usage.postgres;

import edu.berkeley.ground.dao.usage.LineageGraphFactory;
import edu.berkeley.ground.dao.versions.postgres.PostgresItemFactory;
import edu.berkeley.ground.db.DbClient;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.db.PostgresResults;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.usage.LineageGraph;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.util.IdGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresLineageGraphFactory extends LineageGraphFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresLineageGraphFactory.class);
  private final PostgresClient dbClient;
  private final PostgresItemFactory itemFactory;

  private final IdGenerator idGenerator;

  /**
   * Constructor for the Postgres lineage graph factory.
   *
   * @param itemFactory the singleton PostgresItemFactory
   * @param dbClient the Postgres client
   * @param idGenerator a unique id generator
   */
  public PostgresLineageGraphFactory(PostgresItemFactory itemFactory,
                                     PostgresClient dbClient,
                                     IdGenerator idGenerator) {
    this.dbClient = dbClient;
    this.itemFactory = itemFactory;
    this.idGenerator = idGenerator;
  }

  /**
   * Create and persist a lineage graph.
   *
   * @param name the name of the lineage graph
   * @param sourceKey the user generated unique id of the lineage graph
   * @param tags the tags associated with this lineage graph
   * @return the created lineage graph
   * @throws GroundException an unexpected error while creating or persisting this lineage graph
   */
  @Override
  public LineageGraph create(String name, String sourceKey, Map<String, Tag> tags)
      throws GroundException {
    long uniqueId = this.idGenerator.generateItemId();

    this.itemFactory.insertIntoDatabase(uniqueId, tags);

    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("name", GroundType.STRING, name));
    insertions.add(new DbDataContainer("item_id", GroundType.LONG, uniqueId));
    insertions.add(new DbDataContainer("source_key", GroundType.STRING, sourceKey));

    this.dbClient.insert("lineage_graph", insertions);

    LOGGER.info("Created lineage_graph " + name + ".");

    return LineageGraphFactory.construct(uniqueId, name, sourceKey, tags);
  }

  /**
   * Retrieve a lineage graph from the database.
   *
   * @param sourceKey the key of the lineage graph
   * @return the retrieved lineage graph
   * @throws GroundException either the lineage graph doesn't exist or couldn't be retrieved
   */
  @Override
  public LineageGraph retrieveFromDatabase(String sourceKey) throws GroundException {
    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("source_key", GroundType.STRING, sourceKey));

    PostgresResults resultSet;
    try {
      resultSet = this.dbClient.equalitySelect("lineage_graph", DbClient.SELECT_STAR, predicates);
    } catch (EmptyResultException e) {
      throw new GroundException("No LineageGraph found with source_key " + sourceKey + ".");
    }

    long id = resultSet.getLong(1);
    String name = resultSet.getString(3);

    Map<String, Tag> tags = this.itemFactory.retrieveFromDatabase(id).getTags();

    LOGGER.info("Retrieved lineage_graph " + sourceKey + ".");

    return LineageGraphFactory.construct(id, name, sourceKey, tags);
  }

  @Override
  public void update(long itemId, long childId, List<Long> parentIds) throws GroundException {
    this.itemFactory.update(itemId, childId, parentIds);
  }

  @Override
  public void truncate(long itemId, int numLevels) throws GroundException {
    this.itemFactory.truncate(itemId, numLevels, "lineage_graph");
  }
}
