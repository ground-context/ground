/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.api.models.neo4j;

import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.StructureVersionFactory;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundDBException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;

import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.v1.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Neo4jStructureVersionFactory extends StructureVersionFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jStructureVersionFactory.class);
  private final Neo4jClient dbClient;
  private final Neo4jStructureFactory structureFactory;

  private final IdGenerator idGenerator;

  public Neo4jStructureVersionFactory(Neo4jClient dbClient, Neo4jStructureFactory structureFactory, IdGenerator idGenerator) {
    this.dbClient = dbClient;
    this.structureFactory = structureFactory;
    this.idGenerator = idGenerator;
  }

  public StructureVersion create(long structureId, Map<String, GroundType> attributes, List<Long> parentIds) throws GroundException {
    try {
      long id = this.idGenerator.generateVersionId();

      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("id", GroundType.LONG, id));
      insertions.add(new DbDataContainer("structure_id", GroundType.LONG, structureId));

      this.dbClient.addVertex("StructureVersion", insertions);

      for (String key : attributes.keySet()) {
        List<DbDataContainer> itemInsertions = new ArrayList<>();
        itemInsertions.add(new DbDataContainer("svid", GroundType.LONG, id));
        itemInsertions.add(new DbDataContainer("skey", GroundType.STRING, key));
        itemInsertions.add(new DbDataContainer("stype", GroundType.STRING, attributes.get(key).toString()));

        this.dbClient.addVertexAndEdge("StructureVersionItem", itemInsertions, "StructureVersionItemConnection", id, new ArrayList<>());
      }

      this.structureFactory.update(structureId, id, parentIds);

      this.dbClient.commit();
      LOGGER.info("Created structure version " + id + " in structure " + structureId + ".");

      return StructureVersionFactory.construct(id, structureId, attributes);
    } catch (GroundDBException ge) {
      this.dbClient.abort();
      throw ge;
    }
  }

  public StructureVersion retrieveFromDatabase(long id) throws GroundException {
    try {
      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer("id", GroundType.LONG, id));

      long structureId;

      try {
        structureId = this.dbClient.getVertex(predicates).get("v").asNode().get("structure_id").asLong();
      } catch (EmptyResultException e) {
        throw new GroundDBException("No StructureVersion found with id " + id + ".");
      }
      List<String> returnFields = new ArrayList<>();
      returnFields.add("svid");
      returnFields.add("skey");
      returnFields.add("stype");

      List<Record> edges = this.dbClient.getAdjacentVerticesByEdgeLabel("StructureVersionItemConnection", id, returnFields);
      Map<String, GroundType> attributes = new HashMap<>();


      for (Record record : edges) {
        attributes.put(Neo4jClient.getStringFromValue((StringValue) record.get("skey")), GroundType.fromString(Neo4jClient.getStringFromValue((StringValue) record.get("stype"))));
      }

      this.dbClient.commit();
      LOGGER.info("Retrieved structure version " + id + " in structure " + structureId + ".");

      return StructureVersionFactory.construct(id, structureId, attributes);
    } catch (GroundDBException ge) {
      this.dbClient.abort();

      throw ge;
    }
  }
}
