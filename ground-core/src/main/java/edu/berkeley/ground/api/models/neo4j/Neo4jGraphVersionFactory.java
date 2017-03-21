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

import edu.berkeley.ground.api.models.GraphVersion;
import edu.berkeley.ground.api.models.GraphVersionFactory;
import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.Tag;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Neo4jGraphVersionFactory extends GraphVersionFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jGraphVersionFactory.class);
  private final Neo4jClient dbClient;
  private final IdGenerator idGenerator;

  private final Neo4jGraphFactory graphFactory;
  private final Neo4jRichVersionFactory richVersionFactory;

  public Neo4jGraphVersionFactory(Neo4jClient dbClient, Neo4jGraphFactory graphFactory, Neo4jRichVersionFactory richVersionFactory, IdGenerator idGenerator) {
    this.dbClient = dbClient;
    this.graphFactory = graphFactory;
    this.richVersionFactory = richVersionFactory;
    this.idGenerator = idGenerator;
  }

  public GraphVersion create(Map<String, Tag> tags,
                             long structureVersionId,
                             String reference,
                             Map<String, String> referenceParameters,
                             long graphId,
                             List<Long> edgeVersionIds,
                             List<Long> parentIds) throws GroundException {

    try {
      long id = this.idGenerator.generateVersionId();

      tags = tags.values().stream().collect(Collectors.toMap(Tag::getKey, tag -> new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType())));

      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("id", GroundType.LONG, id));
      insertions.add(new DbDataContainer("graph_id", GroundType.LONG, graphId));

      this.dbClient.addVertex("GraphVersion", insertions);

      this.dbClient.commit();

      this.richVersionFactory.insertIntoDatabase(id, tags, structureVersionId, reference, referenceParameters);

      this.dbClient.commit();

      for (long edgeVersionId : edgeVersionIds) {
        this.dbClient.addEdge("GraphVersionEdge", id, edgeVersionId, new ArrayList<>());
      }

      this.graphFactory.update(graphId, id, parentIds);


      this.dbClient.commit();
      LOGGER.info("Created graph version " + id + " in graph " + graphId + ".");

      return GraphVersionFactory.construct(id, tags, structureVersionId, reference, referenceParameters, graphId, edgeVersionIds);
    } catch (GroundDBException e) {
      this.dbClient.abort();

      throw e;
    }
  }

  public GraphVersion retrieveFromDatabase(long id) throws GroundException {
    try {
      RichVersion version = this.richVersionFactory.retrieveFromDatabase(id);

      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer("id", GroundType.LONG, id));

      Record versionRecord;
      try {
        versionRecord = this.dbClient.getVertex(predicates);
      } catch (EmptyResultException e) {
        throw new GroundDBException("No GraphVersion found with id " + id + ".");
      }

      long graphId = versionRecord.get("v") .asNode().get("graph_id").asLong();

      List<String> returnFields = new ArrayList<>();
      returnFields.add("id");

      List<Record> edgeVersionVertices = this.dbClient.getAdjacentVerticesByEdgeLabel("GraphVersionEdge", id, returnFields);
      List<Long> edgeVersionIds = new ArrayList<>();

      edgeVersionVertices.forEach(edgeVersionVertex -> edgeVersionIds.add(edgeVersionVertex.get("id").asLong()));

      this.dbClient.commit();
      LOGGER.info("Retrieved graph version " + id + " in graph " + graphId + ".");

      return GraphVersionFactory.construct(id, version.getTags(), version.getStructureVersionId(), version.getReference(), version.getParameters(), graphId, edgeVersionIds);
    } catch (GroundDBException e) {
      this.dbClient.abort();

      throw e;
    }
  }
}
