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

package edu.berkeley.ground.dao.usage.postgres;

import edu.berkeley.ground.dao.models.RichVersionFactory;
import edu.berkeley.ground.dao.models.postgres.PostgresRichVersionFactory;
import edu.berkeley.ground.dao.models.postgres.PostgresStructureVersionFactory;
import edu.berkeley.ground.dao.models.postgres.PostgresTagFactory;
import edu.berkeley.ground.dao.usage.LineageGraphVersionFactory;
import edu.berkeley.ground.db.DbClient;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.db.PostgresResults;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.RichVersion;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.usage.LineageGraphVersion;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.util.IdGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresLineageGraphVersionFactory
    extends PostgresRichVersionFactory<LineageGraphVersion>
    implements LineageGraphVersionFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(
      PostgresLineageGraphVersionFactory.class);

  private final PostgresClient dbClient;
  private final PostgresLineageGraphFactory lineageGraphFactory;

  private final IdGenerator idGenerator;

  /**
   * Constructor for the Postgres lineage graph factory.
   *
   * @param lineageGraphFactory the singleton PostgresLineageGraphFactory
   * @param dbClient the Postgres client
   * @param idGenerator a unique id generator
   */
  public PostgresLineageGraphVersionFactory(PostgresClient dbClient,
                                            PostgresLineageGraphFactory lineageGraphFactory,
                                            PostgresStructureVersionFactory structureVersionFactory,
                                            PostgresTagFactory tagFactory,
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

    super.insertIntoDatabase(id, tags, structureVersionId, reference, referenceParameters);

    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("id", GroundType.LONG, id));
    insertions.add(new DbDataContainer("lineage_graph_id", GroundType.LONG, lineageGraphId));

    this.dbClient.insert("lineage_graph_version", insertions);

    for (long lineageEdgeVersionId : lineageEdgeVersionIds) {
      List<DbDataContainer> lineageEdgeInsertion = new ArrayList<>();
      lineageEdgeInsertion.add(new DbDataContainer("lineage_graph_version_id", GroundType.LONG,
          id));
      lineageEdgeInsertion.add(new DbDataContainer("lineage_edge_version_id", GroundType.LONG,
          lineageEdgeVersionId));

      this.dbClient.insert("lineage_graph_version_edge", lineageEdgeInsertion);
    }

    this.lineageGraphFactory.update(lineageGraphId, id, parentIds);

    LOGGER.info("Created lineage_graph version " + id + " in lineage_graph " + lineageGraphId
        + ".");

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

    List<DbDataContainer> lineageEdgePredicate = new ArrayList<>();
    lineageEdgePredicate.add(new DbDataContainer("lineage_graph_version_id", GroundType.LONG,
        id));

    PostgresResults resultSet = this.dbClient.equalitySelect("lineage_graph_version",
        DbClient.SELECT_STAR,
        predicates);
    super.verifyResultSet(resultSet, id);

    PostgresResults lineageEdgeSet;
    List<Long> lineageEdgeVersionIds = new ArrayList<>();

    lineageEdgeSet = this.dbClient.equalitySelect("lineage_graph_version_edge", DbClient
        .SELECT_STAR, lineageEdgePredicate);
    if (!lineageEdgeSet.isEmpty()) {
      do {
        lineageEdgeVersionIds.add(lineageEdgeSet.getLong(2));
      } while (lineageEdgeSet.next());
    }

    long lineageGraphId = resultSet.getLong(2);

    LOGGER.info("Retrieved lineage_graph version "
        + id
        + " in lineage_graph "
        + lineageGraphId + ".");

    return new LineageGraphVersion(id, version.getTags(), version.getStructureVersionId(),
        version.getReference(), version.getParameters(), lineageGraphId, lineageEdgeVersionIds);
  }
}
