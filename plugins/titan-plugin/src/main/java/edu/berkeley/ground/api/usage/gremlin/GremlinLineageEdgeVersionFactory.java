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

package edu.berkeley.ground.api.usage.gremlin;

import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.models.gremlin.GremlinRichVersionFactory;
import edu.berkeley.ground.api.usage.LineageEdgeVersion;
import edu.berkeley.ground.api.usage.LineageEdgeVersionFactory;
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
import java.util.stream.Collectors;

public class GremlinLineageEdgeVersionFactory extends LineageEdgeVersionFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(GremlinLineageEdgeVersionFactory.class);
  private GremlinClient dbClient;

  private GremlinLineageEdgeFactory lineageEdgeFactory;
  private GremlinRichVersionFactory richVersionFactory;

  public GremlinLineageEdgeVersionFactory(GremlinLineageEdgeFactory lineageEdgeFactory, GremlinRichVersionFactory richVersionFactory, GremlinClient dbClient) {
    this.dbClient = dbClient;
    this.lineageEdgeFactory = lineageEdgeFactory;
    this.richVersionFactory = richVersionFactory;
  }


  public LineageEdgeVersion create(Map<String, Tag> tags,
                                   String structureVersionId,
                                   String reference,
                                   Map<String, String> referenceParameters,
                                   String fromId,
                                   String toId,
                                   String lineageEdgeId,
                                   List<String> parentIds) throws GroundException {

    GremlinConnection connection = this.dbClient.getConnection();

    try {
      String id = IdGenerator.generateId(lineageEdgeId);

      tags.values().stream().collect(Collectors.toMap(Tag::getKey, tag -> new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType())));

      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("id", GroundType.STRING, id));
      insertions.add(new DbDataContainer("lineageedge_id", GroundType.STRING, lineageEdgeId));
      insertions.add(new DbDataContainer("endpoint_one", GroundType.STRING, fromId));
      insertions.add(new DbDataContainer("endpoint_two", GroundType.STRING, toId));

      Vertex versionVertex = connection.addVertex("LineageEdgeVersions", insertions);

      this.richVersionFactory.insertIntoDatabase(connection, id, tags, structureVersionId, reference, referenceParameters);

      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer("id", GroundType.STRING, fromId));

      Vertex fromVertex = null;
      Vertex toVertex = null;

      try {
        fromVertex = connection.getVertex(predicates);
      } catch (EmptyResultException eer) {
        throw new GroundException("No RichVersion found with id " + fromId + ".");
      }

      predicates.clear();
      predicates.add(new DbDataContainer("id", GroundType.STRING, toId));
      try {
        toVertex = connection.getVertex(predicates);
      } catch (EmptyResultException eer) {
        throw new GroundException("No RichVersion found with id " + toId + ".");
      }

      predicates.clear();
      connection.addEdge("LineageEdgeVersionConnection", fromVertex, versionVertex, predicates);
      connection.addEdge("LineageEdgeVersionConnection", versionVertex, toVertex, predicates);

      this.lineageEdgeFactory.update(connection, lineageEdgeId, id, parentIds);

      connection.commit();
      LOGGER.info("Created lineage edge version " + id + " in lineage edge " + lineageEdgeId + ".");

      return LineageEdgeVersionFactory.construct(id, tags, structureVersionId, reference, referenceParameters, fromId, toId, lineageEdgeId);
    } catch (GroundException e) {
      connection.abort();

      throw e;
    }
  }

  public LineageEdgeVersion retrieveFromDatabase(String id) throws GroundException {
    GremlinConnection connection = this.dbClient.getConnection();

    try {
      RichVersion version = this.richVersionFactory.retrieveFromDatabase(connection, id);

      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer("id", GroundType.STRING, id));

      Vertex versionVertex = null;
      try {
        versionVertex = connection.getVertex(predicates);
      } catch (EmptyResultException eer) {
        throw new GroundException("No LineageEdge found with id " + id + ".");
      }

      String lineageEdgeId = versionVertex.property("lineageedge_id").value().toString();
      String fromId = versionVertex.property("endpoint_one").value().toString();
      String toId = versionVertex.property("endpoint_two").value().toString();

      connection.commit();
      LOGGER.info("Retrieved lineage edge version " + id + " in lineage edge " + lineageEdgeId + ".");

      return LineageEdgeVersionFactory.construct(id, version.getTags(), version.getStructureVersionId(), version.getReference(), version.getParameters(), fromId, toId, lineageEdgeId);
    } catch (GroundException e) {
      connection.abort();

      throw e;
    }
  }
}
