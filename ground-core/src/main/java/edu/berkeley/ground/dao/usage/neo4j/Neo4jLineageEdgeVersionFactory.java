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

package edu.berkeley.ground.dao.usage.neo4j;

import edu.berkeley.ground.dao.models.neo4j.Neo4jRichVersionFactory;
import edu.berkeley.ground.dao.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundDbException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.RichVersion;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.usage.LineageEdgeVersion;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.util.IdGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.driver.v1.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jLineageEdgeVersionFactory extends LineageEdgeVersionFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(
      Neo4jLineageEdgeVersionFactory.class);
  private final Neo4jClient dbClient;
  private final Neo4jLineageEdgeFactory lineageEdgeFactory;
  private final Neo4jRichVersionFactory richVersionFactory;

  private final IdGenerator idGenerator;

  /**
   * Constructor for the Neo4j lineage edge version factory.
   *
   * @param lineageEdgeFactory the singleton Neo4jLineageEdgeFactory
   * @param richVersionFactory the singleton Neo4jRichVersionFactory
   * @param dbClient the Neo4j client
   * @param idGenerator a unique id generator
   */
  public Neo4jLineageEdgeVersionFactory(Neo4jLineageEdgeFactory lineageEdgeFactory,
                                        Neo4jRichVersionFactory richVersionFactory,
                                        Neo4jClient dbClient,
                                        IdGenerator idGenerator) {
    this.dbClient = dbClient;
    this.lineageEdgeFactory = lineageEdgeFactory;
    this.richVersionFactory = richVersionFactory;
    this.idGenerator = idGenerator;
  }

  /**
   * Create and persist a lineage edge version.
   *
   * @param tags the tags associated with this version
   * @param structureVersionId the id of the StructureVersion associated with this version
   * @param reference an optional external reference
   * @param referenceParameters the access parameters for the reference
   * @param fromId the source id of the lineage edge version
   * @param toId the destination id of the lineage edge version
   * @param lineageEdgeId the id of the lineage edge containing this version
   * @param parentIds the ids of the parent(s) of this version
   * @return the created lineage edge version
   * @throws GroundException an error while creating or persisting this version
   */
  public LineageEdgeVersion create(Map<String, Tag> tags,
                                   long structureVersionId,
                                   String reference,
                                   Map<String, String> referenceParameters,
                                   long fromId,
                                   long toId,
                                   long lineageEdgeId,
                                   List<Long> parentIds) throws GroundException {

    try {
      long id = this.idGenerator.generateVersionId();

      tags.values().stream().collect(Collectors.toMap(Tag::getKey, tag ->
          new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType()))
      );

      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("id", GroundType.LONG, id));
      insertions.add(new DbDataContainer("lineageedge_id", GroundType.LONG, lineageEdgeId));
      insertions.add(new DbDataContainer("endpoint_one", GroundType.LONG, fromId));
      insertions.add(new DbDataContainer("endpoint_two", GroundType.LONG, toId));

      this.dbClient.addVertex("LineageEdgeVersions", insertions);

      this.lineageEdgeFactory.update(lineageEdgeId, id, parentIds);
      this.richVersionFactory.insertIntoDatabase(id, tags, structureVersionId, reference,
          referenceParameters);

      this.dbClient.addEdge("LineageEdgeVersionConnection", fromId, id, new ArrayList<>());
      this.dbClient.addEdge("LineageEdgeVersionConnection", id, toId, new ArrayList<>());

      this.dbClient.commit();
      LOGGER.info("Created lineage edge version " + id + " in lineage edge " + lineageEdgeId + ".");

      return LineageEdgeVersionFactory.construct(id, tags, structureVersionId, reference,
          referenceParameters, fromId, toId, lineageEdgeId);
    } catch (GroundDbException e) {
      this.dbClient.abort();

      throw e;
    }
  }

  /**
   * Retrieve a lineage edge version from the database.
   *
   * @param id the id of the version
   * @return the retrieved version
   * @throws GroundException either the version didn't exist or couldn't be retrieved
   */
  public LineageEdgeVersion retrieveFromDatabase(long id) throws GroundException {
    try {
      final RichVersion version = this.richVersionFactory.retrieveFromDatabase(id);

      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer("id", GroundType.LONG, id));

      Record versionRecord;
      try {
        versionRecord = this.dbClient.getVertex(predicates);
      } catch (EmptyResultException e) {
        throw new GroundDbException("No LineageEdgeVersion found with id " + id + ".");
      }

      long lineageEdgeId = versionRecord. get("v").asNode().get("lineageedge_id").asLong();
      long fromId = versionRecord.get("v").asNode().get("endpoint_one").asLong();
      long toId = versionRecord.get("v").asNode().get("endpoint_two").asLong();

      this.dbClient.commit();
      LOGGER.info("Retrieved lineage edge version " + id + " in lineage edge " + lineageEdgeId
          + ".");

      return LineageEdgeVersionFactory.construct(id, version.getTags(),
          version.getStructureVersionId(), version.getReference(), version.getParameters(), fromId,
          toId, lineageEdgeId);
    } catch (GroundDbException e) {
      this.dbClient.abort();

      throw e;
    }
  }
}
