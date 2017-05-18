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

import dao.models.NodeVersionFactory;
import dao.models.RichVersionFactory;
import db.CassandraClient;
import db.CassandraResults;
import db.DbClient;
import db.DbDataContainer;
import exceptions.GroundException;
import models.models.NodeVersion;
import models.models.RichVersion;
import models.models.Tag;
import models.versions.GroundType;
import util.IdGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CassandraNodeVersionFactory
    extends CassandraRichVersionFactory<NodeVersion>
    implements NodeVersionFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraNodeVersionFactory.class);
  private final CassandraClient dbClient;
  private final CassandraNodeFactory nodeFactory;

  private final IdGenerator idGenerator;

  /**
   * Constructor for the Cassandra node version factory.
   *
   * @param nodeFactory the singleton CassandraNodeFactory
   * @param dbClient the Cassandra client
   * @param idGenerator a unique id generator
   */
  public CassandraNodeVersionFactory(CassandraClient dbClient,
                                     CassandraNodeFactory nodeFactory,
                                     CassandraStructureVersionFactory structureVersionFactory,
                                     CassandraTagFactory tagFactory,
                                     IdGenerator idGenerator) {

    super(dbClient, structureVersionFactory, tagFactory);

    this.dbClient = dbClient;
    this.nodeFactory = nodeFactory;
    this.idGenerator = idGenerator;
  }


  /**
   * Create and persist a node version.
   *
   * @param tags tags associated with this version
   * @param structureVersionId the id of the StructureVersion associated with this version
   * @param reference an optional external reference
   * @param referenceParameters access parameters for the reference
   * @param nodeId the id of the node containing this version
   * @param parentIds the ids of the parent(s) of this version
   * @return the newly created version
   * @throws GroundException an error while creating or persisting the version
   */
  @Override
  public NodeVersion create(Map<String, Tag> tags,
                            long structureVersionId,
                            String reference,
                            Map<String, String> referenceParameters,
                            long nodeId,
                            List<Long> parentIds) throws GroundException {

    long id = this.idGenerator.generateVersionId();

    // add the id of the version to the tag
    tags = RichVersionFactory.addIdToTags(id, tags);
    super.insertIntoDatabase(id, tags, structureVersionId, reference, referenceParameters);

    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("id", GroundType.LONG, id));
    insertions.add(new DbDataContainer("node_id", GroundType.LONG, nodeId));

    this.dbClient.insert("node_version", insertions);

    this.nodeFactory.update(nodeId, id, parentIds);

    LOGGER.info("Created node version " + id + " in node " + nodeId + ".");
    return new NodeVersion(id, tags, structureVersionId, reference, referenceParameters, nodeId);
  }

  /**
   * Retrieve a node version from the database.
   *
   * @param id the id of the version to retrieve
   * @return the retrieved version
   * @throws GroundException either the version doesn't exist or couldn't be retrieved
   */
  @Override
  public NodeVersion retrieveFromDatabase(long id) throws GroundException {
    final RichVersion version = super.retrieveRichVersionData(id);

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("id", GroundType.LONG, id));

    CassandraResults resultSet = this.dbClient.equalitySelect("node_version",
        DbClient.SELECT_STAR,
        predicates);
    super.verifyResultSet(resultSet, id);


    long nodeId = resultSet.getLong("node_id");

    LOGGER.info("Retrieved node version " + id + " in node " + nodeId + ".");
    return new NodeVersion(id, version.getTags(), version.getStructureVersionId(),
        version.getReference(), version.getParameters(), nodeId);
  }
}
