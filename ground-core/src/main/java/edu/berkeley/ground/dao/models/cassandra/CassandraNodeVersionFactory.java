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

package edu.berkeley.ground.dao.models.cassandra;

import edu.berkeley.ground.model.models.NodeVersion;
import edu.berkeley.ground.dao.models.NodeVersionFactory;
import edu.berkeley.ground.model.models.RichVersion;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.QueryResults;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CassandraNodeVersionFactory extends NodeVersionFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraNodeVersionFactory.class);
  private final CassandraClient dbClient;
  private final CassandraNodeFactory nodeFactory;
  private final CassandraRichVersionFactory richVersionFactory;

  private final IdGenerator idGenerator;

  public CassandraNodeVersionFactory(CassandraNodeFactory nodeFactory, CassandraRichVersionFactory richVersionFactory, CassandraClient dbClient, IdGenerator idGenerator) {
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

    try {
      long id = this.idGenerator.generateVersionId();

      // add the id of the version to the tag
      tags = tags.values().stream().collect(Collectors.toMap(Tag::getKey, tag -> new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType())));

      this.richVersionFactory.insertIntoDatabase(id, tags, structureVersionId, reference, referenceParameters);

      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("id", GroundType.LONG, id));
      insertions.add(new DbDataContainer("node_id", GroundType.LONG, nodeId));

      this.dbClient.insert("node_version", insertions);

      this.nodeFactory.update(nodeId, id, parentIds);

      this.dbClient.commit();
      LOGGER.info("Created node version " + id + " in node " + nodeId + ".");

      return NodeVersionFactory.construct(id, tags, structureVersionId, reference, referenceParameters, nodeId);
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }
  }

  public NodeVersion retrieveFromDatabase(long id) throws GroundException {
    try {
      RichVersion version = this.richVersionFactory.retrieveFromDatabase(id);

      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer("id", GroundType.LONG, id));

      QueryResults resultSet;
      try {
        resultSet = this.dbClient.equalitySelect("node_version", DBClient.SELECT_STAR, predicates);
      } catch (EmptyResultException e) {
        this.dbClient.abort();

        throw new GroundException("No NodeVersion found with id " + id + ".");
      }

      if (!resultSet.next()) {
        this.dbClient.abort();

        throw new GroundException("No NodeVersion found with id " + id + ".");
      }

      long nodeId = resultSet.getLong(1);

      this.dbClient.commit();
      LOGGER.info("Retrieved node version " + id + " in node " + nodeId + ".");

      return NodeVersionFactory.construct(id, version.getTags(), version.getStructureVersionId(), version.getReference(), version.getParameters(), nodeId);
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }
  }

  public List<Long> getAdjacentNodes(long nodeVersionId, String edgeNameRegex) {
    List<Long> result = this.dbClient.adjacentNodes(nodeVersionId, edgeNameRegex);

    this.dbClient.commit();
    return result;
  }
}
