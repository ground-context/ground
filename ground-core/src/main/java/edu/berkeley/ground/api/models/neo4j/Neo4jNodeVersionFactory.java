/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.api.models.neo4j;

import edu.berkeley.ground.api.models.NodeVersion;
import edu.berkeley.ground.api.models.NodeVersionFactory;
import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.db.Neo4jClient.Neo4jConnection;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;

import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.v1.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Neo4jNodeVersionFactory extends NodeVersionFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jNodeVersionFactory.class);
  private Neo4jClient dbClient;
  private IdGenerator idGenerator;

  private Neo4jNodeFactory nodeFactory;
  private Neo4jRichVersionFactory richVersionFactory;

  public Neo4jNodeVersionFactory(Neo4jNodeFactory nodeFactory, Neo4jRichVersionFactory richVersionFactory, Neo4jClient dbClient, IdGenerator idGenerator) {
    this.dbClient = dbClient;
    this.nodeFactory = nodeFactory;
    this.richVersionFactory = richVersionFactory;
    this.idGenerator = idGenerator;
  }

  public NodeVersion create(Map<String, Tag> tags,
                            long structureVersionId,
                            String reference,
                            Map<String, String> referenceParameters,
                            long nodeId,
                            List<Long> parentIds) throws GroundException {

    Neo4jConnection connection = this.dbClient.getConnection();

    try {
      long id = this.idGenerator.generateVersionId();

      // add the id of the version to the tag
      tags = tags.values().stream().collect(Collectors.toMap(Tag::getKey, tag -> new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType())));

      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("id", GroundType.LONG, id));
      insertions.add(new DbDataContainer("node_id", GroundType.LONG, nodeId));

      connection.addVertex("NodeVersion", insertions);
      this.richVersionFactory.insertIntoDatabase(connection, id, tags, structureVersionId, reference, referenceParameters);

      this.nodeFactory.update(connection, nodeId, id, parentIds);

      connection.commit();

      LOGGER.info("Created node version " + id + " in node " + nodeId + ".");

      return NodeVersionFactory.construct(id, tags, structureVersionId, reference, referenceParameters, nodeId);
    } catch (GroundException e) {
      connection.abort();

      throw e;
    }
  }

  public NodeVersion retrieveFromDatabase(long id) throws GroundException {
    Neo4jConnection connection = this.dbClient.getConnection();

    try {
      RichVersion version = this.richVersionFactory.retrieveFromDatabase(connection, id);

      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer("id", GroundType.LONG, id));

      Record record;
      try {
        record = connection.getVertex(predicates);
      } catch (EmptyResultException eer) {
        throw new GroundException("No NodeVersion found with id " + id + ".");
      }

      long nodeId = record.get("v").asNode(). get("node_id").asLong();

      connection.commit();
      LOGGER.info("Retrieved node version " + id + " in node " + nodeId + ".");

      return NodeVersionFactory.construct(id, version.getTags(), version.getStructureVersionId(), version.getReference(), version.getParameters(), nodeId);
    } catch (GroundException e) {
      connection.abort();

      throw e;
    }
  }

  public List<Long> getTransitiveClosure(long nodeVersionId) throws GroundException {
    Neo4jConnection connection = this.dbClient.getConnection();
    try {
      List<Long> result = connection.transitiveClosure(nodeVersionId);

      connection.commit();
      return result;
    } catch (GroundException e) {
      connection.abort();

      throw e;
    }
  }

  public List<Long> getAdjacentNodes(long nodeVersionId, String edgeNameRegex) throws GroundException {
    Neo4jConnection connection = this.dbClient.getConnection();
    List<Long> result = connection.adjacentNodes(nodeVersionId, edgeNameRegex);

    connection.commit();
    return result;
  }
}
