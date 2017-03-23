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

package edu.berkeley.ground.api.models.cassandra;

import edu.berkeley.ground.api.models.EdgeVersion;
import edu.berkeley.ground.api.models.EdgeVersionFactory;
import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
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

public class CassandraEdgeVersionFactory extends EdgeVersionFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraEdgeVersionFactory.class);
  private final CassandraClient dbClient;

  private final CassandraEdgeFactory edgeFactory;
  private final CassandraRichVersionFactory richVersionFactory;
  private final IdGenerator idGenerator;

  public CassandraEdgeVersionFactory(CassandraEdgeFactory edgeFactory, CassandraRichVersionFactory richVersionFactory, CassandraClient dbClient, IdGenerator idGenerator) {
    this.dbClient = dbClient;
    this.edgeFactory = edgeFactory;
    this.richVersionFactory = richVersionFactory;
    this.idGenerator = idGenerator;
  }

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
    try {
      long id = this.idGenerator.generateVersionId();

      tags = tags.values().stream().collect(Collectors.toMap(Tag::getKey, tag -> new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType())));

      this.richVersionFactory.insertIntoDatabase(id, tags, structureVersionId, reference, referenceParameters);

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

      this.dbClient.commit();
      LOGGER.info("Created edge version " + id + " in edge " + edgeId + ".");

      return EdgeVersionFactory.construct(id, tags, structureVersionId, reference,
          referenceParameters, edgeId, fromNodeVersionStartId, fromNodeVersionEndId,
          toNodeVersionStartId, toNodeVersionEndId);
    } catch (GroundException e) {
      this.dbClient.abort();
      throw e;
    }
  }

  public EdgeVersion retrieveFromDatabase(long id) throws GroundException {
    try {
      RichVersion version = this.richVersionFactory.retrieveFromDatabase(id);

      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer("id", GroundType.LONG, id));

      QueryResults resultSet;
      try {
        resultSet = this.dbClient.equalitySelect("edge_version", DBClient.SELECT_STAR, predicates);
      } catch (EmptyResultException e) {
        this.dbClient.abort();

        throw new GroundException("No EdgeVersion found with id " + id + ".");
      }

      if (!resultSet.next()) {
        this.dbClient.abort();

        throw new GroundException("No EdgeVersion found with id " + id + ".");
      }

      long edgeId = resultSet.getLong("edge_id");

      long fromNodeVersionStartId = resultSet.getLong("from_node_start_id");
      long fromNodeVersionEndId = resultSet.isNull("from_node_end_id") ? -1 : resultSet.getLong
          ("from_node_end_id");
      long toNodeVersionStartId = resultSet.getLong("to_node_start_id");
      long toNodeVersionEndId = resultSet.isNull("to_node_end_id") ? -1 : resultSet.getLong
          ("to_node_end_id");

      this.dbClient.commit();
      LOGGER.info("Retrieved edge version " + id + " in Edge " + edgeId + ".");

      return EdgeVersionFactory.construct(id, version.getTags(), version.getStructureVersionId(),
          version.getReference(), version.getParameters(), edgeId, fromNodeVersionStartId,
          fromNodeVersionEndId, toNodeVersionStartId, toNodeVersionEndId);
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }
  }

  protected void updatePreviousVersion(long id, long fromEndId, long toEndId)
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
