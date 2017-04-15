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

import edu.berkeley.ground.dao.models.NodeVersionFactory;
import edu.berkeley.ground.dao.models.RichVersionFactory;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.NodeVersion;
import edu.berkeley.ground.model.models.RichVersion;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.util.IdGenerator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Neo4jNodeVersionFactory
    extends Neo4jRichVersionFactory<NodeVersion>
    implements NodeVersionFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jNodeVersionFactory.class);
  private final Neo4jClient dbClient;
  private final IdGenerator idGenerator;

  private final Neo4jNodeFactory nodeFactory;

  /**
   * Constructor for the Neo4j node version factory.
   *
   * @param nodeFactory the singleton Neo4jNodeFactory
   * @param dbClient the Neo4j client
   * @param idGenerator a unique id generator
   */
  public Neo4jNodeVersionFactory(Neo4jClient dbClient,
                                 Neo4jNodeFactory nodeFactory,
                                 Neo4jStructureVersionFactory structureVersionFactory,
                                 Neo4jTagFactory tagFactory,
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

    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("id", GroundType.LONG, id));
    insertions.add(new DbDataContainer("node_id", GroundType.LONG, nodeId));

    this.dbClient.addVertex("NodeVersion", insertions);
    super.insertIntoDatabase(id, tags, structureVersionId, reference, referenceParameters);

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

    Record record = this.dbClient.getVertex(predicates);
    super.verifyResultSet(record, id);

    long nodeId = record.get("v").asNode(). get("node_id").asLong();
    LOGGER.info("Retrieved node version " + id + " in node " + nodeId + ".");

    return new NodeVersion(id, version.getTags(), version.getStructureVersionId(),
        version.getReference(), version.getParameters(), nodeId);
  }
}
