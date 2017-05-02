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

package dao.usage.postgres;

import dao.models.RichVersionFactory;
import dao.models.postgres.PostgresRichVersionFactory;
import dao.models.postgres.PostgresStructureVersionFactory;
import dao.models.postgres.PostgresTagFactory;
import dao.usage.LineageEdgeVersionFactory;
import db.DbClient;
import db.DbCondition;
import db.DbEqualsCondition;
import db.DbResults;
import db.DbRow;
import db.PostgresClient;
import exceptions.GroundException;
import models.models.RichVersion;
import models.models.Tag;
import models.usage.LineageEdgeVersion;
import models.versions.GroundType;
import util.IdGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresLineageEdgeVersionFactory
    extends PostgresRichVersionFactory<LineageEdgeVersion>
    implements LineageEdgeVersionFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(
      PostgresLineageEdgeVersionFactory.class);
  private final PostgresClient dbClient;
  private final PostgresLineageEdgeFactory lineageEdgeFactory;

  private final IdGenerator idGenerator;

  /**
   * Constructor for the Postgres lineage edge version factory.
   *
   * @param lineageEdgeFactory the singleton PostgresLineageEdgeFactory
   * @param dbClient the Postgres client
   * @param idGenerator a unique id generator
   */
  public PostgresLineageEdgeVersionFactory(PostgresClient dbClient,
                                           PostgresLineageEdgeFactory lineageEdgeFactory,
                                           PostgresStructureVersionFactory structureVersionFactory,
                                           PostgresTagFactory tagFactory,
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

    super.insertIntoDatabase(id, tags, structureVersionId, reference, referenceParameters);

    List<DbEqualsCondition> insertions = new ArrayList<>();
    insertions.add(new DbEqualsCondition("id", GroundType.LONG, id));
    insertions.add(new DbEqualsCondition("lineage_edge_id", GroundType.LONG, lineageEdgeId));
    insertions.add(new DbEqualsCondition("from_rich_version_id", GroundType.LONG, fromId));
    insertions.add(new DbEqualsCondition("to_rich_version_id", GroundType.LONG, toId));

    this.dbClient.insert("lineage_edge_version", insertions);

    this.lineageEdgeFactory.update(lineageEdgeId, id, parentIds);

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

    List<DbCondition> predicates = new ArrayList<>();
    predicates.add(new DbEqualsCondition("id", GroundType.LONG, id));

    DbResults resultSet = this.dbClient.select("lineage_edge_version",
        DbClient.SELECT_STAR, predicates);
    super.verifyResultSet(resultSet, id);

    DbRow row = resultSet.one();
    long lineageEdgeId = row.getLong("lineage_edge_id");
    long fromId = row.getLong("from_rich_version_id");
    long toId = row.getLong("to_rich_version_id");

    LOGGER.info("Retrieved lineage edge version " + id + " in lineage edge " + lineageEdgeId
        + ".");

    return new LineageEdgeVersion(id, version.getTags(), version.getStructureVersionId(),
        version.getReference(), version.getParameters(), fromId, toId, lineageEdgeId);
  }
}
