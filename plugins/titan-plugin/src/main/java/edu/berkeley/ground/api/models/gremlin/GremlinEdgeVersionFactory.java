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

import edu.berkeley.ground.api.models.EdgeVersion;
import edu.berkeley.ground.api.models.EdgeVersionFactory;
import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.Tag;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class GremlinEdgeVersionFactory extends EdgeVersionFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(GremlinEdgeVersionFactory.class);
  private GremlinClient dbClient;

  private GremlinEdgeFactory edgeFactory;
  private GremlinRichVersionFactory richVersionFactory;

  public GremlinEdgeVersionFactory(GremlinEdgeFactory edgeFactory, GremlinRichVersionFactory richVersionFactory, GremlinClient dbClient) {
    this.dbClient = dbClient;
    this.edgeFactory = edgeFactory;
    this.richVersionFactory = richVersionFactory;
  }

  public EdgeVersion create(Map<String, Tag> tags,
                            String structureVersionId,
                            String reference,
                            Map<String, String> referenceParameters,
                            String edgeId,
                            String fromId,
                            String toId,
                            List<String> parentIds) throws GroundException {

    GremlinConnection connection = this.dbClient.getConnection();

    try {
      String id = IdGenerator.generateId(edgeId);

      tags = tags.values().stream().collect(Collectors.toMap(Tag::getKey, tag -> new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType())));


      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("id", GroundType.STRING, id));
      insertions.add(new DbDataContainer("edge_id", GroundType.STRING, edgeId));
      insertions.add(new DbDataContainer("endpoint_one", GroundType.STRING, fromId));
      insertions.add(new DbDataContainer("endpoint_two", GroundType.STRING, toId));

      Vertex vertex = connection.addVertex("EdgeVersion", insertions);
      this.richVersionFactory.insertIntoDatabase(connection, id, tags, structureVersionId, reference, referenceParameters);

      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer("id", GroundType.STRING, fromId));
      Vertex fromVertex;
      Vertex toVertex;
      try {
        fromVertex = connection.getVertex(predicates);
      } catch (EmptyResultException eer) {
        throw new GroundException("No NodeVersion found with id " + fromId + ".");
      }

      predicates.clear();
      predicates.add(new DbDataContainer("id", GroundType.STRING, toId));
      try {
        toVertex = connection.getVertex(predicates);
      } catch (EmptyResultException eer) {
        throw new GroundException("No NodeVersion found with id " + toId + ".");
      }

      predicates.clear();
      connection.addEdge("EdgeVersionConnection", fromVertex, vertex, predicates);
      connection.addEdge("EdgeVersionConnection", vertex, toVertex, predicates);

      this.edgeFactory.update(connection, edgeId, id, parentIds);

      connection.commit();
      LOGGER.info("Created edge version " + id + " in edge " + edgeId + ".");

      return EdgeVersionFactory.construct(id, tags, structureVersionId, reference, referenceParameters, edgeId, fromId, toId);
    } catch (GroundException e) {
      connection.abort();
      throw e;
    }
  }

  public EdgeVersion retrieveFromDatabase(String id) throws GroundException {
    GremlinConnection connection = this.dbClient.getConnection();

    try {
      RichVersion version = this.richVersionFactory.retrieveFromDatabase(connection, id);

      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer("id", GroundType.STRING, id));

      Vertex versionVertex = null;
      try {
        versionVertex = connection.getVertex(predicates);
      } catch (EmptyResultException eer) {
        throw new GroundException("No EdgeVersion found with id " + id + ".");
      }

      String edgeId = versionVertex.property("edge_id").value().toString();
      String fromId = versionVertex.property("endpoint_one").value().toString();
      String toId = versionVertex.property("endpoint_two").value().toString();

      connection.commit();
      LOGGER.info("Retrieved edge version " + id + " in edge " + edgeId + ".");

      return EdgeVersionFactory.construct(id, version.getTags(), version.getStructureVersionId(), version.getReference(), version.getParameters(), edgeId, fromId, toId);
    } catch (GroundException e) {
      connection.abort();

      throw e;
    }
  }
}
