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

import edu.berkeley.ground.dao.models.RichVersionFactory;
import edu.berkeley.ground.dao.models.neo4j.Neo4jRichVersionFactory;
import edu.berkeley.ground.dao.models.neo4j.Neo4jStructureVersionFactory;
import edu.berkeley.ground.dao.models.neo4j.Neo4jTagFactory;
import edu.berkeley.ground.dao.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.RichVersion;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.usage.LineageEdgeVersion;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.util.IdGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jLineageEdgeVersionFactory
    extends Neo4jRichVersionFactory<LineageEdgeVersion>
    implements LineageEdgeVersionFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(
      Neo4jLineageEdgeVersionFactory.class);
  private final Neo4jClient dbClient;
  private final Neo4jLineageEdgeFactory lineageEdgeFactory;

  private final IdGenerator idGenerator;

  /**
   * Constructor for the Neo4j lineage edge version factory.
   *
   * @param lineageEdgeFactory the singleton Neo4jLineageEdgeFactory
   * @param dbClient the Neo4j client
   * @param idGenerator a unique id generator
   */
  public Neo4jLineageEdgeVersionFactory(Neo4jClient dbClient,
                                        Neo4jLineageEdgeFactory lineageEdgeFactory,
                                        Neo4jStructureVersionFactory structureVersionFactory,
                                        Neo4jTagFactory tagFactory,
                                        IdGenerator idGenerator) {

    super(dbClient, structureVersionFactory, tagFactory);

    this.dbClient = dbClient;
    this.lineageEdgeFactory = lineageEdgeFactory;
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
  @Override
  public LineageEdgeVersion create(Map<String, Tag> tags,
                                   long structureVersionId,
                                   String reference,
                                   Map<String, String> referenceParameters,
                                   long fromId,
                                   long toId,
                                   long lineageEdgeId,
                                   List<Long> parentIds) throws GroundException {

    long id = this.idGenerator.generateVersionId();

    tags = RichVersionFactory.addIdToTags(id, tags);

    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("id", GroundType.LONG, id));
    insertions.add(new DbDataContainer("lineageedge_id", GroundType.LONG, lineageEdgeId));
    insertions.add(new DbDataContainer("endpoint_one", GroundType.LONG, fromId));
    insertions.add(new DbDataContainer("endpoint_two", GroundType.LONG, toId));

    this.dbClient.addVertex("LineageEdgeVersion", insertions);

    this.lineageEdgeFactory.update(lineageEdgeId, id, parentIds);
    super.insertIntoDatabase(id, tags, structureVersionId, reference, referenceParameters);

    this.dbClient.addEdge("LineageEdgeVersionConnection", fromId, id, new ArrayList<>());
    this.dbClient.addEdge("LineageEdgeVersionConnection", id, toId, new ArrayList<>());

    LOGGER.info("Created lineage edge version " + id + " in lineage edge " + lineageEdgeId + ".");
    return new LineageEdgeVersion(id, tags, structureVersionId, reference, referenceParameters,
        fromId, toId, lineageEdgeId);
  }

  /**
   * Retrieve a lineage edge version from the database.
   *
   * @param id the id of the version
   * @return the retrieved version
   * @throws GroundException either the version didn't exist or couldn't be retrieved
   */
  @Override
  public LineageEdgeVersion retrieveFromDatabase(long id) throws GroundException {
    final RichVersion version = super.retrieveRichVersionData(id);

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("id", GroundType.LONG, id));

    Record versionRecord = this.dbClient.getVertex(predicates);
    super.verifyResultSet(versionRecord, id);

    long lineageEdgeId = versionRecord. get("v").asNode().get("lineageedge_id").asLong();
    long fromId = versionRecord.get("v").asNode().get("endpoint_one").asLong();
    long toId = versionRecord.get("v").asNode().get("endpoint_two").asLong();

    LOGGER.info("Retrieved lineage edge version " + id + " in lineage edge " + lineageEdgeId
        + ".");
    return new LineageEdgeVersion(id, version.getTags(), version.getStructureVersionId(),
        version.getReference(), version.getParameters(), fromId, toId, lineageEdgeId);
  }
}
