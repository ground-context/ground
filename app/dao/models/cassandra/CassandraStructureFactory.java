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

package dao.models.cassandra;

import dao.models.StructureFactory;
import dao.versions.cassandra.CassandraItemFactory;
import dao.versions.cassandra.CassandraVersionHistoryDagFactory;
import db.CassandraClient;
import db.DbClient;
import db.DbDataContainer;
import db.DbResults;
import db.DbRow;
import exceptions.GroundException;
import models.models.Structure;
import models.models.Tag;
import models.versions.GroundType;
import util.IdGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraStructureFactory
    extends CassandraItemFactory<Structure>
    implements StructureFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraStructureFactory.class);
  private final CassandraClient dbClient;

  private final IdGenerator idGenerator;

  /**
   * Constructor for the Cassandra structure factory.
   *
   * @param dbClient the Cassandra client
   * @param idGenerator a unique id generator
   */
  public CassandraStructureFactory(CassandraClient dbClient,
                                   CassandraVersionHistoryDagFactory versionHistoryDagFactory,
                                   CassandraTagFactory tagFactory,
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
   * Retrieves a structure from the database.
   *
   * @param id the id of the structure to retrieve
   * @return the retrieved structure
   * @throws GroundException either the structure doesn't exist or couldn't be retrieved
   */
  @Override
  public Structure retrieveFromDatabase(long id) throws GroundException {
    return this.retrieveByPredicate("id", id, GroundType.LONG);
  }

  private Structure retrieveByPredicate(String fieldName, Object value, GroundType valueType)
      throws GroundException {

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer(fieldName, valueType, value));

    DbResults resultSet = this.dbClient.equalitySelect("structure",
        DbClient.SELECT_STAR, predicates);
    super.verifyResultSet(resultSet, fieldName, value);

    DbRow row = resultSet.one();
    long id = row.getLong("item_id");
    String sourceKey = row.getString("source_key");
    String name = row.getString("name");

    Map<String, Tag> tags = super.retrieveItemTags(id);

    LOGGER.info("Retrieved structure " + value + ".");
    return new Structure(id, name, sourceKey, tags);
  }
}
