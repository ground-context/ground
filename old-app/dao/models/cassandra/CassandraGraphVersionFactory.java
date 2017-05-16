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

package dao.models.cassandra;

import dao.models.GraphVersionFactory;
import dao.models.RichVersionFactory;
import db.CassandraClient;
import db.CassandraResults;
import db.DbClient;
import db.DbDataContainer;
import exceptions.GroundException;
import models.models.GraphVersion;
import models.models.RichVersion;
import models.models.Tag;
import models.versions.GroundType;
import util.IdGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraGraphVersionFactory
    extends CassandraRichVersionFactory<GraphVersion>
    implements GraphVersionFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraGraphVersionFactory.class);
  private final CassandraClient dbClient;
  private final CassandraGraphFactory graphFactory;

  private final IdGenerator idGenerator;

  /**
   * Constructor for the Cassandra graph version factory.
   *
   * @param graphFactory the singleton CassandraGraphFactory
   * @param dbClient the CassandraClient
   * @param idGenerator a unique ID generator
   */
  public CassandraGraphVersionFactory(CassandraClient dbClient,
                                      CassandraGraphFactory graphFactory,
                                      CassandraStructureVersionFactory structureVersionFactory,
                                      CassandraTagFactory tagFactory,
                                      IdGenerator idGenerator) {
    super(dbClient, structureVersionFactory, tagFactory);

    this.dbClient = dbClient;
    this.graphFactory = graphFactory;
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

    long id = this.idGenerator.generateVersionId();
    tags = RichVersionFactory.addIdToTags(id, tags);

    super.insertIntoDatabase(id, tags, structureVersionId, reference, referenceParameters);

    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("id", GroundType.LONG, id));
    insertions.add(new DbDataContainer("graph_id", GroundType.LONG, graphId));

    this.dbClient.insert("graph_version", insertions);

    for (long edgeVersionId : edgeVersionIds) {
      List<DbDataContainer> edgeInsertion = new ArrayList<>();
      edgeInsertion.add(new DbDataContainer("graph_version_id", GroundType.LONG, id));
      edgeInsertion.add(new DbDataContainer("edge_version_id", GroundType.LONG, edgeVersionId));

      this.dbClient.insert("graph_version_edge", edgeInsertion);
    }

    this.graphFactory.update(graphId, id, parentIds);

    LOGGER.info("Created graph version " + id + " in graph " + graphId + ".");
    return new GraphVersion(id, tags, structureVersionId, reference, referenceParameters,
        graphId, edgeVersionIds);
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
    final RichVersion version = super.retrieveRichVersionData(id);

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("id", GroundType.LONG, id));

    List<DbDataContainer> edgePredicate = new ArrayList<>();
    edgePredicate.add(new DbDataContainer("graph_version_id", GroundType.LONG, id));

    CassandraResults resultSet = this.dbClient.equalitySelect("graph_version",
        DbClient.SELECT_STAR,
        predicates);


    long graphId = resultSet.getLong("graph_id");

    List<Long> edgeVersionIds = new ArrayList<>();
    CassandraResults edgeSet = this.dbClient.equalitySelect("graph_version_edge",
        DbClient.SELECT_STAR,
        edgePredicate);

    if (!edgeSet.isEmpty()) {
      do {
        edgeVersionIds.add(edgeSet.getLong("edge_version_id"));
      } while (edgeSet.next());
    }

    LOGGER.info("Retrieved graph version " + id + " in graph " + graphId + ".");
    return new GraphVersion(id, version.getTags(), version.getStructureVersionId(),
        version.getReference(), version.getParameters(), graphId, edgeVersionIds);
  }
}
