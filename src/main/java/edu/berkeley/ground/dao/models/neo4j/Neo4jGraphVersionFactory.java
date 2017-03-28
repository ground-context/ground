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

import edu.berkeley.ground.dao.models.GraphVersionFactory;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundDbException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.GraphVersion;
import edu.berkeley.ground.model.models.RichVersion;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.util.IdGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.driver.v1.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jGraphVersionFactory extends GraphVersionFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jGraphVersionFactory.class);
  private final Neo4jClient dbClient;
  private final IdGenerator idGenerator;

  private final Neo4jGraphFactory graphFactory;
  private final Neo4jRichVersionFactory richVersionFactory;

  /**
   * Constructor for the Cassandra graph version factory.
   *
   * @param graphFactory the singleton Neo4jGraphFactory
   * @param richVersionFactory the singleton Neo4jRichVersionFactory
   * @param dbClient the Neo4jClient
   * @param idGenerator a unique ID generator
   */
  public Neo4jGraphVersionFactory(Neo4jClient dbClient,
                                  Neo4jGraphFactory graphFactory,
                                  Neo4jRichVersionFactory richVersionFactory,
                                  IdGenerator idGenerator) {
    this.dbClient = dbClient;
    this.graphFactory = graphFactory;
    this.richVersionFactory = richVersionFactory;
    this.idGenerator = idGenerator;
  }

  /**
   * Create and persist a graph version.
   *
   * @param tags tags associated with this graph version
   * @param structureVersionId the id of the StructureVersion associated with this version
   * @param reference an optional external reference
   * @param referenceParameters access parameters for the reference
   * @param graphId the id of the graph containing this version
   * @param edgeVersionIds the list of edge versions in this graph version
   * @param parentIds the ids of the parent(s) of this version
   * @return the created graph version
   * @throws GroundException an error while creating or persisting the graph
   */
  @Override
  public GraphVersion create(Map<String, Tag> tags,
                             long structureVersionId,
                             String reference,
                             Map<String, String> referenceParameters,
                             long graphId,
                             List<Long> edgeVersionIds,
                             List<Long> parentIds) throws GroundException {

    try {
      long id = this.idGenerator.generateVersionId();

      tags = tags.values().stream().collect(Collectors.toMap(Tag::getKey, tag ->
          new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType()))
      );

      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("id", GroundType.LONG, id));
      insertions.add(new DbDataContainer("graph_id", GroundType.LONG, graphId));

      this.dbClient.addVertex("GraphVersion", insertions);
      this.richVersionFactory.insertIntoDatabase(id, tags, structureVersionId, reference,
          referenceParameters);

      this.dbClient.commit();

      for (long edgeVersionId : edgeVersionIds) {
        this.dbClient.addEdge("GraphVersionEdge", id, edgeVersionId, new ArrayList<>());
      }

      this.graphFactory.update(graphId, id, parentIds);


      this.dbClient.commit();
      LOGGER.info("Created graph version " + id + " in graph " + graphId + ".");

      return GraphVersionFactory.construct(id, tags, structureVersionId, reference,
          referenceParameters, graphId, edgeVersionIds);
    } catch (GroundDbException e) {
      this.dbClient.abort();

      throw e;
    }
  }

  /**
   * Retrieve a graph version from the database.
   *
   * @param id the id of the graph version to retrieve
   * @return the retrieved graph version
   * @throws GroundException either the graph version doesn't exist or couldn't be retrieved
   */
  @Override
  public GraphVersion retrieveFromDatabase(long id) throws GroundException {
    try {
      final RichVersion version = this.richVersionFactory.retrieveFromDatabase(id);

      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer("id", GroundType.LONG, id));

      Record versionRecord;
      try {
        versionRecord = this.dbClient.getVertex(predicates);
      } catch (EmptyResultException e) {
        throw new GroundDbException("No GraphVersion found with id " + id + ".");
      }

      List<String> returnFields = new ArrayList<>();
      returnFields.add("id");

      List<Record> edgeVersionVertices = this.dbClient
          .getAdjacentVerticesByEdgeLabel("GraphVersionEdge", id, returnFields);
      List<Long> edgeVersionIds = new ArrayList<>();

      edgeVersionVertices.forEach(edgeVersionVertex ->
          edgeVersionIds.add(edgeVersionVertex.get("id").asLong())
      );

      long graphId = versionRecord.get("v") .asNode().get("graph_id").asLong();
      this.dbClient.commit();

      LOGGER.info("Retrieved graph version " + id + " in graph " + graphId + ".");

      return GraphVersionFactory.construct(id, version.getTags(), version.getStructureVersionId(),
          version.getReference(), version.getParameters(), graphId, edgeVersionIds);
    } catch (GroundDbException e) {
      this.dbClient.abort();

      throw e;
    }
  }
}
