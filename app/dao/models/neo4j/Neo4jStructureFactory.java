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

import dao.models.StructureFactory;
import dao.versions.neo4j.Neo4jItemFactory;
import dao.versions.neo4j.Neo4jVersionHistoryDagFactory;
import db.DbEqualsCondition;
import db.Neo4jClient;
import exceptions.GroundException;
import models.models.Structure;
import models.models.Tag;
import models.versions.GroundType;
import util.IdGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jStructureFactory extends Neo4jItemFactory<Structure> implements StructureFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jStructureFactory.class);
  private final Neo4jClient dbClient;

  private final IdGenerator idGenerator;

  /**
   * Constructor for the Neo4j structure factory.
   *
   * @param dbClient the Neo4j client
   * @param idGenerator a unique id generator
   */
  public Neo4jStructureFactory(Neo4jClient dbClient,
                               Neo4jVersionHistoryDagFactory versionHistoryDagFactory,
                               Neo4jTagFactory tagFactory,
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

    List<DbEqualsCondition> insertions = new ArrayList<>();
    insertions.add(new DbEqualsCondition("name", GroundType.STRING, name));
    insertions.add(new DbEqualsCondition("id", GroundType.LONG, uniqueId));
    insertions.add(new DbEqualsCondition("source_key", GroundType.STRING, sourceKey));

    this.dbClient.addVertex("Structure", insertions);

    LOGGER.info("Created structure " + name + ".");
    super.insertIntoDatabase(uniqueId, tags);

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
   * @param id the key of the structure
   * @return the retrieved structure
   * @throws GroundException either the structure doesn't exist or couldn't be retrieved
   */
  @Override
  public Structure retrieveFromDatabase(long id) throws GroundException {
    return this.retrieveByPredicate("id", id, GroundType.STRING);
  }

  private Structure retrieveByPredicate(String fieldName, Object value, GroundType valueType)
      throws GroundException {

    List<DbEqualsCondition> predicates = new ArrayList<>();
    predicates.add(new DbEqualsCondition(fieldName, valueType, value));

    Record record = this.dbClient.getVertex("Structure", predicates);
    super.verifyResultSet(record, fieldName, value);

    long id = record.get("v").asNode().get("id").asLong();
    String name = record.get("v").asNode().get("name").asString();
    String sourceKey = record.get("v").asNode().get("source_key").asString();

    Map<String, Tag> tags = super.retrieveItemTags(id);

    LOGGER.info("Retrieved structure " + value + ".");

    return new Structure(id, name, sourceKey, tags);
  }
}
