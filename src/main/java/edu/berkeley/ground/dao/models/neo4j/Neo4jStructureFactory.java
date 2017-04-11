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

import edu.berkeley.ground.dao.models.StructureFactory;
import edu.berkeley.ground.dao.versions.neo4j.Neo4jItemFactory;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundDbException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.Structure;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.util.IdGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jStructureFactory extends StructureFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jStructureFactory.class);
  private final Neo4jClient dbClient;
  private final Neo4jItemFactory itemFactory;

  private final IdGenerator idGenerator;

  /**
   * Constructor for the Neo4j structure factory.
   *
   * @param itemFactory the singleton Neo4jItemFactory
   * @param dbClient the Neo4j client
   * @param idGenerator a unique id generator
   */
  public Neo4jStructureFactory(Neo4jClient dbClient,
                               Neo4jItemFactory itemFactory,
                               IdGenerator idGenerator) {
    this.dbClient = dbClient;
    this.itemFactory = itemFactory;
    this.idGenerator = idGenerator;
  }

  /**
   * Create and persist a structure.
   *
   * @param name the name of the structure
   * @param sourceKey the user generated unique key for this structure
   * @param tags the tags associated with this structurej
   * @return the created structure
   * @throws GroundException an error while creating or persisting the structure
   */
  @Override
  public Structure create(String name, String sourceKey, Map<String, Tag> tags)
      throws GroundException {
    long uniqueId = this.idGenerator.generateItemId();

    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("name", GroundType.STRING, name));
    insertions.add(new DbDataContainer("id", GroundType.LONG, uniqueId));
    insertions.add(new DbDataContainer("source_key", GroundType.STRING, sourceKey));

    this.dbClient.addVertex("Structure", insertions);

    LOGGER.info("Created structure " + name + ".");
    this.itemFactory.insertIntoDatabase(uniqueId, tags);

    return StructureFactory.construct(uniqueId, name, sourceKey, tags);
  }

  /**
   * Retrieve the leaves of this structure's DAG.
   *
   * @param sourceKey the key of the structure
   * @return the list of leaves in this structure's DAG
   * @throws GroundException an error while retrieving the structure
   */
  @Override
  public List<Long> getLeaves(String sourceKey) throws GroundException {
    Structure structure = this.retrieveFromDatabase(sourceKey);
    List<Long> leaves = this.itemFactory.getLeaves(structure.getId());

    return leaves;
  }

  /**
   * Retrieve a structure from the database.
   *
   * @param sourceKey the key of the structure
   * @return the retrieved structure
   * @throws GroundException either the structure doesn't exist or couldn't be retrieved
   */
  @Override
  public Structure retrieveFromDatabase(String sourceKey) throws GroundException {
    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("source_key", GroundType.STRING, sourceKey));

    Record record;
    try {
      record = this.dbClient.getVertex("Structure", predicates);
    } catch (EmptyResultException e) {
      throw new GroundDbException("No Structure found with source_key " + sourceKey + ".");
    }

    long id = record.get("v").asNode().get("id").asLong();
    String name = record.get("v").asNode().get("name").asString();

    Map<String, Tag> tags = this.itemFactory.retrieveFromDatabase(id).getTags();

    LOGGER.info("Retrieved structure " + sourceKey + ".");

    return StructureFactory.construct(id, name, sourceKey, tags);
  }

  @Override
  public void update(long itemId, long childId, List<Long> parentIds) throws GroundException {
    this.itemFactory.update(itemId, childId, parentIds);
  }

  @Override
  public void truncate(long itemId, int numLevels) throws GroundException {
    this.itemFactory.truncate(itemId, numLevels, "structure");
  }
}
