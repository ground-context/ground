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

import edu.berkeley.ground.dao.models.StructureFactory;
import edu.berkeley.ground.dao.versions.postgres.PostgresItemFactory;
import edu.berkeley.ground.dao.versions.postgres.PostgresVersionHistoryDagFactory;
import edu.berkeley.ground.db.DbClient;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.db.PostgresResults;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.Structure;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.util.IdGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresStructureFactory
    extends PostgresItemFactory<Structure>
    implements StructureFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresStructureFactory.class);
  private final PostgresClient dbClient;

  private final IdGenerator idGenerator;

  /**
   * Constructor for the Postgres structure factory.
   *
   * @param dbClient the Postgres client
   * @param idGenerator a unique id generator
   */
  public PostgresStructureFactory(PostgresClient dbClient,
                                  PostgresVersionHistoryDagFactory versionHistoryDagFactory,
                                  PostgresTagFactory tagFactory,
                                  IdGenerator idGenerator) {

    super(dbClient, versionHistoryDagFactory, tagFactory);

    this.dbClient = dbClient;
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
    super.verifyItemNotExists(sourceKey);
    long uniqueId = this.idGenerator.generateItemId();

    super.insertIntoDatabase(uniqueId, tags);

    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("name", GroundType.STRING, name));
    insertions.add(new DbDataContainer("item_id", GroundType.LONG, uniqueId));
    insertions.add(new DbDataContainer("source_key", GroundType.STRING, sourceKey));

    this.dbClient.insert("structure", insertions);

    LOGGER.info("Created structure " + name + ".");
    return new Structure(uniqueId, name, sourceKey, tags);
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
    return super.getLeaves(structure.getId());
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
    return this.retrieveByPredicate("source_key", sourceKey, GroundType.STRING);
  }

  /**
   * Retrieve a structure from the database.
   *
   * @param id the id of the structure
   * @return the retrieved structure
   * @throws GroundException either the structure doesn't exist or couldn't be retrieved
   */
  @Override
  public Structure retrieveFromDatabase(long id) throws GroundException {
    return this.retrieveByPredicate("source_key", id, GroundType.LONG);
  }

  private Structure retrieveByPredicate(String fieldName, Object value, GroundType valueType)
      throws GroundException {

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer(fieldName, valueType, value));

    PostgresResults resultSet = this.dbClient.equalitySelect("structure",
        DbClient.SELECT_STAR,
        predicates);
    super.verifyResultSet(resultSet, fieldName, value);

    long id = resultSet.getLong(1);
    String sourceKey = resultSet.getString(2);
    String name = resultSet.getString(3);

    Map<String, Tag> tags = super.retrieveItemTags(id);

    LOGGER.info("Retrieved structure " + value + ".");
    return new Structure(id, name, sourceKey, tags);
  }
}
