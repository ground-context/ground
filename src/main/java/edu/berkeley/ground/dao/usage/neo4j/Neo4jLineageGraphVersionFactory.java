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
import edu.berkeley.ground.dao.usage.LineageGraphVersionFactory;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.RichVersion;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.usage.LineageGraphVersion;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.util.IdGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jLineageGraphVersionFactory
    extends Neo4jRichVersionFactory<LineageGraphVersion>
    implements LineageGraphVersionFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(
      Neo4jLineageGraphVersionFactory.class);
  private final Neo4jClient dbClient;
  private final IdGenerator idGenerator;

  private final Neo4jLineageGraphFactory lineageGraphFactory;

  /**
   * Constructor for the Neo4j lineage graph factory.
   *
   * @param lineageGraphFactory the singleton Neo4jLineageGraphFactory
   * @param dbClient the Neo4j client
   * @param idGenerator a unique id generator
   */
  public Neo4jLineageGraphVersionFactory(Neo4jClient dbClient,
                                         Neo4jLineageGraphFactory lineageGraphFactory,
                                         Neo4jStructureVersionFactory structureVersionFactory,
                                         Neo4jTagFactory tagFactory,
                                         IdGenerator idGenerator) {

    super(dbClient, structureVersionFactory, tagFactory);

    this.dbClient = dbClient;
    this.lineageGraphFactory = lineageGraphFactory;
    this.idGenerator = idGenerator;
  }

  /**
   * Create and persist a lineage graph version.
   *
   * @param tags the tags asscoiated with this version
   * @param structureVersionId the id of the StructureVersion associated with this version
   * @param reference an optional external reference
   * @param referenceParameters access parameters for the reference
   * @param lineageGraphId the id of the lineage graph containing this version
   * @param lineageEdgeVersionIds the ids of the lineage edge versions in this lineage graph version
   * @param parentIds the ids of the parent(s) of this version
   * @return the created lineage graph version
   * @throws GroundException an error while creating or persisting this version
   */
  @Override
  public LineageGraphVersion create(Map<String, Tag> tags,
                                    long structureVersionId,
                                    String reference,
                                    Map<String, String> referenceParameters,
                                    long lineageGraphId,
                                    List<Long> lineageEdgeVersionIds,
                                    List<Long> parentIds) throws GroundException {

    long id = this.idGenerator.generateVersionId();

    tags = RichVersionFactory.addIdToTags(id, tags);

    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("id", GroundType.LONG, id));
    insertions.add(new DbDataContainer("lineage_graph_id", GroundType.LONG, lineageGraphId));

    this.dbClient.addVertex("LineageGraphVersion", insertions);
    super.insertIntoDatabase(id, tags, structureVersionId, reference, referenceParameters);

    for (long lineageEdgeVersionId : lineageEdgeVersionIds) {
      this.dbClient.addEdge("LineageGraphVersionEdge", id, lineageEdgeVersionId,
          new ArrayList<>());
    }

    this.lineageGraphFactory.update(lineageGraphId, id, parentIds);


    LOGGER.info("Created graph version " + id + " in graph " + lineageGraphId + ".");

    return new LineageGraphVersion(id, tags, structureVersionId, reference, referenceParameters,
        lineageGraphId, lineageEdgeVersionIds);
  }

  /**
   * Retrieve a lineage graph version from the database.
   *
   * @param id the id of the version
   * @return the retrieved version
   * @throws GroundException either the version doesn't exist or couldn't be retrieved
   */
  @Override
  public LineageGraphVersion retrieveFromDatabase(long id) throws GroundException {
    final RichVersion version = super.retrieveRichVersionData(id);

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("id", GroundType.LONG, id));

    Record versionRecord = this.dbClient.getVertex(predicates);
    super.verifyResultSet(versionRecord, id);

    List<String> returnFields = new ArrayList<>();
    returnFields.add("id");

    List<Record> lineageEdgeVersionVertices = this.dbClient.getAdjacentVerticesByEdgeLabel(
        "LineageGraphVersionEdge", id, returnFields);
    List<Long> lineageEdgeVersionIds = new ArrayList<>();

    lineageEdgeVersionVertices.forEach(edgeVersionVertex -> lineageEdgeVersionIds.add(
        edgeVersionVertex.get("id").asLong()));

    long lineageGraphId = versionRecord.get("v") .asNode().get("lineage_graph_id").asLong();

    LOGGER.info("Retrieved lineage graph version " + id + " in lineage graph " + lineageGraphId
        + ".");
    return new LineageGraphVersion(id, version.getTags(), version.getStructureVersionId(),
        version.getReference(), version.getParameters(), lineageGraphId, lineageEdgeVersionIds);
  }
}
