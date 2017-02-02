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

package edu.berkeley.ground.api.models.gremlin;

import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.StructureVersionFactory;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.GremlinClient;
import edu.berkeley.ground.db.GremlinClient.GremlinConnection;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class GremlinStructureVersionFactory extends StructureVersionFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(GremlinStructureVersionFactory.class);
  private GremlinClient dbClient;

  private GremlinStructureFactory structureFactory;

  public GremlinStructureVersionFactory(GremlinStructureFactory structureFactory, GremlinClient dbClient) {
    this.dbClient = dbClient;
    this.structureFactory = structureFactory;
  }

  public StructureVersion create(String structureId,
                                 Map<String, GroundType> attributes,
                                 List<String> parentIds) throws GroundException {

    GremlinConnection connection = this.dbClient.getConnection();
    String id = IdGenerator.generateId(structureId);

    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("id", GroundType.STRING, id));
    insertions.add(new DbDataContainer("structure_id", GroundType.STRING, structureId));

    Vertex versionVertex = connection.addVertex("StructureVersion", insertions);

    for (String key : attributes.keySet()) {
      List<DbDataContainer> itemInsertions = new ArrayList<>();
      itemInsertions.add(new DbDataContainer("svid", GroundType.STRING, id));
      itemInsertions.add(new DbDataContainer("skey", GroundType.STRING, key));
      itemInsertions.add(new DbDataContainer("type", GroundType.STRING, attributes.get(key).toString()));

      Vertex itemVertex = connection.addVertex("StructureVersionItem", itemInsertions);
      connection.addEdge("StructureVersionItemConnection", versionVertex, itemVertex, new ArrayList<>());
    }

    this.structureFactory.update(connection, structureId, id, parentIds);

    connection.commit();
    LOGGER.info("Created structure version " + id + " in structure " + structureId + ".");

    return StructureVersionFactory.construct(id, structureId, attributes);
  }

  public StructureVersion retrieveFromDatabase(String id) throws GroundException {
    GremlinConnection connection = this.dbClient.getConnection();

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("id", GroundType.STRING, id));

    Vertex versionVertex = null;
    try {
      versionVertex = connection.getVertex(predicates);
    } catch (EmptyResultException eer) {
      throw new GroundException("No StructureVersion found with id " + id + ".");
    }

    List<Vertex> adjacentVetices = connection.getAdjacentVerticesByEdgeLabel(versionVertex, "StructureVersionItemConnection");
    Map<String, GroundType> attributes = new HashMap<>();

    for (Vertex gremlinVertex : adjacentVetices) {
      attributes.put(gremlinVertex.property("skey").value().toString(), GroundType.fromString(gremlinVertex.property("type").value().toString()));
    }

    String structureId = versionVertex.property("structure_id").value().toString();

    connection.commit();
    LOGGER.info("Retrieved structure version " + id + " in structure " + structureId + ".");

    return StructureVersionFactory.construct(id, structureId, attributes);
  }
}
