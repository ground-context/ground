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

package edu.berkeley.ground.dao.models.cassandra;

import edu.berkeley.ground.dao.models.EdgeVersionFactory;
import edu.berkeley.ground.dao.models.RichVersionFactory;
import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.db.CassandraResults;
import edu.berkeley.ground.db.DbClient;
import edu.berkeley.ground.db.DbDataContainer;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraEdgeVersionFactory
    extends CassandraRichVersionFactory<EdgeVersion>
    implements EdgeVersionFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraEdgeVersionFactory.class);
  private final CassandraClient dbClient;

  private final CassandraEdgeFactory edgeFactory;
  private final IdGenerator idGenerator;

  /**
   * Constructor for the Cassandra edge version factory.
   *
   * @param edgeFactory the CassandraEdgeFactory singleton
   * @param dbClient the Cassandra client
   * @param idGenerator a unique ID generator
   */
  public CassandraEdgeVersionFactory(CassandraClient dbClient,
                                     CassandraEdgeFactory edgeFactory,
                                     CassandraStructureVersionFactory structureVersionFactory,
                                     CassandraTagFactory tagFactory,
                                     IdGenerator idGenerator) {

    super(dbClient, structureVersionFactory, tagFactory);

    this.dbClient = dbClient;
    this.edgeFactory = edgeFactory;
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
  @Override
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
    long id = this.idGenerator.generateVersionId();
    tags = RichVersionFactory.addIdToTags(id, tags);

    super.insertIntoDatabase(id, tags, structureVersionId, reference, referenceParameters);

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

    this.dbClient.insert("edge_version", insertions);
    this.edgeFactory.update(edgeId, id, parentIds);
    LOGGER.info("Created edge version " + id + " in edge " + edgeId + ".");

    return new EdgeVersion(id, tags, structureVersionId, reference, referenceParameters,
        edgeId, fromNodeVersionStartId, fromNodeVersionEndId, toNodeVersionStartId,
        toNodeVersionEndId);
  }

  /**
   * Retrieves an edge version from the database.
   *
   * @param id the id of the version to retrieve
   * @return the retrieved edge version
   * @throws GroundException either the version doesn't exist or wasn't able to be retrieved
   */
  @Override
  public EdgeVersion retrieveFromDatabase(long id) throws GroundException {
    final RichVersion version = super.retrieveRichVersionData(id);

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("id", GroundType.LONG, id));

    CassandraResults resultSet = this.dbClient.equalitySelect("edge_version",
        DbClient.SELECT_STAR,
        predicates);
    super.verifyResultSet(resultSet, id);

    long edgeId = resultSet.getLong("edge_id");

    long fromNodeVersionStartId = resultSet.getLong("from_node_start_id");
    long fromNodeVersionEndId = resultSet.isNull("from_node_end_id") ? -1 : resultSet.getLong(
        "from_node_end_id");
    long toNodeVersionStartId = resultSet.getLong("to_node_start_id");
    long toNodeVersionEndId = resultSet.isNull("to_node_end_id") ? -1 : resultSet.getLong(
        "to_node_end_id");

    LOGGER.info("Retrieved edge version " + id + " in Edge " + edgeId + ".");

    return new EdgeVersion(id, version.getTags(), version.getStructureVersionId(),
        version.getReference(), version.getParameters(), edgeId, fromNodeVersionStartId,
        fromNodeVersionEndId, toNodeVersionStartId, toNodeVersionEndId);
  }

  @Override
  public void updatePreviousVersion(long id, long fromEndId, long toEndId)
      throws GroundException {

    List<DbDataContainer> setPredicates = new ArrayList<>();
    List<DbDataContainer> wherePredicates = new ArrayList<>();

    if (fromEndId != -1) {
      setPredicates.add(new DbDataContainer("from_node_end_id", GroundType.LONG, fromEndId));
    }

    if (toEndId != -1) {
      setPredicates.add(new DbDataContainer("to_node_end_id", GroundType.LONG, toEndId));
    }

    wherePredicates.add(new DbDataContainer("id", GroundType.LONG, id));
    this.dbClient.update(setPredicates, wherePredicates, "edge_version");
  }
}
