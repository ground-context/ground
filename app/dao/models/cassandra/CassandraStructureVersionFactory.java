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

import dao.models.StructureVersionFactory;
import dao.versions.cassandra.CassandraVersionFactory;
import db.CassandraClient;
import db.CassandraResults;
import db.DbClient;
import db.DbDataContainer;
import exceptions.GroundException;
import models.models.StructureVersion;
import models.versions.GroundType;
import util.IdGenerator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CassandraStructureVersionFactory
    extends CassandraVersionFactory<StructureVersion>
    implements StructureVersionFactory {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(CassandraStructureVersionFactory.class);

  private final CassandraClient dbClient;
  private final CassandraStructureFactory structureFactory;

  private final IdGenerator idGenerator;

  /**
   * Constructor for the Cassandra structure version factory.
   *
   * @param structureFactory the singleton CassandraStructureFactory
   * @param dbClient the Cassandra client
   * @param idGenerator a unique id generator
   */
  public CassandraStructureVersionFactory(CassandraClient dbClient,
                                          CassandraStructureFactory structureFactory,
                                          IdGenerator idGenerator) {

    super(dbClient);

    this.dbClient = dbClient;
    this.structureFactory = structureFactory;
    this.idGenerator = idGenerator;
  }

  /**
   * Create and persist a structure version.
   *
   * @param structureId the id of the structure containing this version
   * @param attributes the attributes required by this structure version
   * @param parentIds the ids of the parent(s) of this version
   * @return the created structure version
   * @throws GroundException an error while creating or persisting this version
   */
  @Override
  public StructureVersion create(long structureId,
                                 Map<String, GroundType> attributes,
                                 List<Long> parentIds) throws GroundException {

    long id = this.idGenerator.generateVersionId();

    super.insertIntoDatabase(id);

    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("id", GroundType.LONG, id));
    insertions.add(new DbDataContainer("structure_id", GroundType.LONG, structureId));

    this.dbClient.insert("structure_version", insertions);

    for (String key : attributes.keySet()) {
      Map<String, String> map = new HashMap<>();
      map.put(key, attributes.get(key).toString());
      List<DbDataContainer> predicate = new ArrayList<>();
      predicate.add(new DbDataContainer("id", GroundType.LONG, id));
      this.dbClient.addToMap("structure_version", "key_type_map", map, predicate);
    }

    this.structureFactory.update(structureId, id, parentIds);

    LOGGER.info("Created structure version " + id + " in structure " + structureId + ".");
    return new StructureVersion(id, structureId, attributes);
  }

  /**
   * Retrieve a structure version from the database.
   *
   * @param id the id of the version to retrieve
   * @return the retrieved version
   * @throws GroundException either the version doesn't exist or couldn't be retrieved
   */
  @Override
  public StructureVersion retrieveFromDatabase(long id) throws GroundException {
    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("id", GroundType.LONG, id));

    CassandraResults resultSet = this.dbClient.equalitySelect("structure_version",
        DbClient.SELECT_STAR,
        predicates);
    super.verifyResultSet(resultSet, id);

    Map<String, String> tmpAttributes = resultSet.getMap("key_type_map", String.class, String.class);

    if (tmpAttributes.isEmpty()) {
      throw new GroundException("No StructureVersion attributes found for id " + id + ".");
    }

    Map<String, GroundType> attributes = new HashMap<>();
    for (String key : tmpAttributes.keySet()) {
      attributes.put(key, GroundType.fromString(tmpAttributes.get(key)));
    }

    long structureId = resultSet.getLong("structure_id");

    LOGGER.info("Retrieved structure version " + id + " in structure " + structureId + ".");
    return new StructureVersion(id, structureId, attributes);
  }
}
