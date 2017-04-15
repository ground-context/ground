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

import edu.berkeley.ground.dao.models.StructureVersionFactory;
import edu.berkeley.ground.dao.versions.neo4j.Neo4jVersionFactory;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.exceptions.GroundDbException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.StructureVersion;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.util.IdGenerator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.v1.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jStructureVersionFactory
    extends Neo4jVersionFactory<StructureVersion>
    implements StructureVersionFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jStructureVersionFactory.class);
  private final Neo4jClient dbClient;
  private final Neo4jStructureFactory structureFactory;

  private final IdGenerator idGenerator;

  /**
   * Constructor for the Neo4j structure version factory.
   *
   * @param structureFactory the singleton Neo4jStructureFactory
   * @param dbClient the Neo4j client
   * @param idGenerator a unique id generator
   */
  public Neo4jStructureVersionFactory(Neo4jClient dbClient,
                                      Neo4jStructureFactory structureFactory,
                                      IdGenerator idGenerator) {
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

    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("id", GroundType.LONG, id));
    insertions.add(new DbDataContainer("structure_id", GroundType.LONG, structureId));

    this.dbClient.addVertex("StructureVersion", insertions);

    for (String key : attributes.keySet()) {
      List<DbDataContainer> itemInsertions = new ArrayList<>();
      itemInsertions.add(new DbDataContainer("svid", GroundType.LONG, id));
      itemInsertions.add(new DbDataContainer("skey", GroundType.STRING, key));
      itemInsertions.add(new DbDataContainer("stype", GroundType.STRING,
          attributes.get(key).toString()));

      this.dbClient.addVertexAndEdge("StructureVersionItem", itemInsertions,
          "StructureVersionItemConnection", id, new ArrayList<>());
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

    Record record = this.dbClient.getVertex(predicates);
    super.verifyResultSet(record, id);

    long structureId = record.get("v").asNode().get("structure_id").asLong();

    List<String> returnFields = new ArrayList<>();
    returnFields.add("svid");
    returnFields.add("skey");
    returnFields.add("stype");

    List<Record> edges = this.dbClient.getAdjacentVerticesByEdgeLabel(
        "StructureVersionItemConnection", id, returnFields);
    Map<String, GroundType> attributes = new HashMap<>();


    for (Record edge : edges) {
      attributes.put(Neo4jClient.getStringFromValue((StringValue) edge.get("skey")),
          GroundType.fromString(Neo4jClient.getStringFromValue((StringValue) edge.get("stype")))
      );
    }

    LOGGER.info("Retrieved structure version " + id + " in structure " + structureId + ".");

    return new StructureVersion(id, structureId, attributes);
  }
}
