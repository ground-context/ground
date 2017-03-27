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

import edu.berkeley.ground.dao.models.EdgeVersionFactory;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundDbException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.EdgeVersion;
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

public class Neo4jEdgeVersionFactory extends EdgeVersionFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jEdgeVersionFactory.class);
  private final Neo4jClient dbClient;
  private final IdGenerator idGenerator;

  private final Neo4jEdgeFactory edgeFactory;
  private final Neo4jRichVersionFactory richVersionFactory;

  /**
   * Constructor for the Neo4j edge version factory.
   *
   * @param edgeFactory the Neo4jEdgeFactory singleton
   * @param richVersionFactory the Neo4jRichVersionFactory singleton
   * @param dbClient the Neo4j client
   * @param idGenerator a unique ID generator
   */
  public Neo4jEdgeVersionFactory(Neo4jEdgeFactory edgeFactory,
                                 Neo4jRichVersionFactory richVersionFactory,
                                 Neo4jClient dbClient,
                                 IdGenerator idGenerator) {
    this.dbClient = dbClient;
    this.edgeFactory = edgeFactory;
    this.richVersionFactory = richVersionFactory;
    this.idGenerator = idGenerator;
  }

  /**
   * Creates and persists a new edge version.
   *
   * @param tags any tags on the edge version
   * @param structureVersionId the id of the StructureVersion for this version
   * @param reference an optional external reference
   * @param referenceParameters parameters to access this reference
   * @param edgeId the id of the Edge containing this version
   * @param fromNodeVersionStartId the start id in the Edge's from node
   * @param fromNodeVersionEndId the end id in the Edge's from node
   * @param toNodeVersionStartId the start id in the Edge's to node
   * @param toNodeVersionEndId the end id in the Edge's to node
   * @param parentIds the ids of any parents of this version
   * @return the created version
   * @throws GroundException an error while creating or persisting the version
   */
  public EdgeVersion create(Map<String, Tag> tags,
                            long structureVersionId,
                            String reference,
                            Map<String, String> referenceParameters,
                            long edgeId,
                            long fromNodeVersionStartId,
                            long fromNodeVersionEndId,
                            long toNodeVersionStartId,
                            long toNodeVersionEndId,
                            List<Long> parentIds) throws GroundException {
    try {
      long id = idGenerator.generateVersionId();

      tags = tags.values().stream().collect(Collectors.toMap(Tag::getKey, tag ->
          new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType()))
      );

      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("id", GroundType.LONG, id));
      insertions.add(new DbDataContainer("edge_id", GroundType.LONG, edgeId));
      insertions.add(new DbDataContainer("from_node_start_id", GroundType.LONG,
          fromNodeVersionStartId));
      insertions.add(new DbDataContainer("from_node_end_id", GroundType.LONG,
          fromNodeVersionEndId));
      insertions.add(new DbDataContainer("to_node_start_id", GroundType.LONG,
          toNodeVersionStartId));
      insertions.add(new DbDataContainer("to_node_end_id", GroundType.LONG, toNodeVersionEndId));

      this.dbClient.addVertex("EdgeVersion", insertions);
      this.richVersionFactory.insertIntoDatabase(id, tags, structureVersionId, reference,
          referenceParameters);

      this.dbClient.addEdge("EdgeVersionConnection", fromNodeVersionStartId, id, new ArrayList<>());
      this.dbClient.addEdge("EdgeVersionConnection", id, toNodeVersionStartId, new ArrayList<>());

      if (fromNodeVersionEndId != -1) {
        this.dbClient.addEdge("EdgeVersionConnection", fromNodeVersionEndId, id, new ArrayList<>());
      }

      if (toNodeVersionEndId != -1) {
        this.dbClient.addEdge("EdgeVersionConnection", toNodeVersionEndId, id, new ArrayList<>());
      }

      this.edgeFactory.update(edgeId, id, parentIds);

      this.dbClient.commit();
      LOGGER.info("Created edge version " + id + " in edge " + edgeId + ".");

      return EdgeVersionFactory.construct(id, tags, structureVersionId, reference,
          referenceParameters, edgeId, fromNodeVersionStartId, fromNodeVersionEndId,
          toNodeVersionStartId, toNodeVersionEndId);
    } catch (GroundDbException e) {
      this.dbClient.abort();
      throw e;
    }
  }

  /**
   * Retrieves an edge version from the database.
   *
   * @param id the id of the version to retrieve
   * @return the retrieved edge version
   * @throws GroundException either the version doesn't exist or wasn't able to be retrieved
   */
  public EdgeVersion retrieveFromDatabase(long id) throws GroundException {
    try {
      final RichVersion version = this.richVersionFactory.retrieveFromDatabase(id);

      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer("id", GroundType.LONG, id));

      Record versionRecord;
      try {
        versionRecord = this.dbClient.getVertex("EdgeVersion", predicates);
      } catch (EmptyResultException e) {
        throw new GroundDbException("No EdgeVersion found with id " + id + ".");
      }

      long edgeId = versionRecord.get("v").asNode() .get("edge_id").asLong();
      long fromNodeVersionStartId = versionRecord.get("v").asNode().get("from_node_start_id")
          .asLong();
      long fromNodeVersionEndId = versionRecord.get("v").asNode().get("from_node_end_id")
          .asLong();
      long toNodeVersionStartId = versionRecord.get("v").asNode().get("to_node_start_id")
          .asLong();
      long toNodeVersionEndId = versionRecord.get("v").asNode().get("to_node_end_id")
          .asLong();

      this.dbClient.commit();
      LOGGER.info("Retrieved edge version " + id + " in edge " + edgeId + ".");

      return EdgeVersionFactory.construct(id, version.getTags(), version.getStructureVersionId(),
          version.getReference(), version.getParameters(), edgeId, fromNodeVersionStartId,
          fromNodeVersionEndId, toNodeVersionStartId, toNodeVersionEndId);
    } catch (GroundDbException e) {
      this.dbClient.abort();

      throw e;
    }
  }

  protected void updatePreviousVersion(long id, long fromEndId, long toEndId)
      throws GroundException {

    if (fromEndId != -1) {
      this.dbClient.setProperty(id, "from_node_end_id", fromEndId, false);
      this.dbClient.addEdge("EdgeVersionConnection", fromEndId, id, new ArrayList<>());
    }

    if (toEndId != -1) {
      this.dbClient.setProperty(id, "to_node_end_id", toEndId, false);
      this.dbClient.addEdge("EdgeVersionConnection", id, toEndId, new ArrayList<>());
    }
  }
}
