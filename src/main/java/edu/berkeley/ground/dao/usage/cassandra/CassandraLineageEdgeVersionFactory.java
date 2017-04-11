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

package edu.berkeley.ground.dao.usage.cassandra;

import edu.berkeley.ground.dao.models.RichVersionFactory;
import edu.berkeley.ground.dao.models.cassandra.CassandraRichVersionFactory;
import edu.berkeley.ground.dao.models.cassandra.CassandraStructureVersionFactory;
import edu.berkeley.ground.dao.models.cassandra.CassandraTagFactory;
import edu.berkeley.ground.dao.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.db.CassandraResults;
import edu.berkeley.ground.db.DbClient;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.RichVersion;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.usage.LineageEdgeVersion;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.util.IdGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraLineageEdgeVersionFactory
    extends CassandraRichVersionFactory<LineageEdgeVersion>
    implements LineageEdgeVersionFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(
      CassandraLineageEdgeVersionFactory.class);
  private final CassandraClient dbClient;
  private final CassandraLineageEdgeFactory lineageEdgeFactory;

  private final IdGenerator idGenerator;

  /**
   * Constructor for the Cassandra lineage edge version factory.
   *
   * @param lineageEdgeFactory the singleton CassandraLineageEdgeFactory
   * @param dbClient the Cassandra client
   * @param idGenerator a unique id generator
   */
  public CassandraLineageEdgeVersionFactory(
      CassandraClient dbClient,
      CassandraLineageEdgeFactory lineageEdgeFactory,
      CassandraStructureVersionFactory structureVersionFactory,
      CassandraTagFactory tagFactory,
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

    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("id", GroundType.LONG, id));
    insertions.add(new DbDataContainer("lineage_edge_id", GroundType.LONG, lineageEdgeId));
    insertions.add(new DbDataContainer("from_rich_version_id", GroundType.LONG, fromId));
    insertions.add(new DbDataContainer("to_rich_version_id", GroundType.LONG, toId));

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

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("id", GroundType.LONG, id));

    CassandraResults resultSet = this.dbClient.equalitySelect("lineage_edge_version",
        DbClient.SELECT_STAR,
        predicates);
    super.verifyResultSet(resultSet, id);

    long lineageEdgeId = resultSet.getLong("lineage_edge_id");
    long fromId = resultSet.getLong("from_rich_version_id");
    long toId = resultSet.getLong("to_rich_version_id");

    LOGGER.info("Retrieved lineage edge version " + id + " in lineage edge " + lineageEdgeId
        + ".");
    return new LineageEdgeVersion(id, version.getTags(), version.getStructureVersionId(),
        version.getReference(), version.getParameters(), fromId, toId, lineageEdgeId);
  }
}
