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

import edu.berkeley.ground.api.models.EdgeVersion;
import edu.berkeley.ground.api.models.EdgeVersionFactory;
import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundDBException;
import edu.berkeley.ground.util.IdGenerator;

import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.v1.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Neo4jEdgeVersionFactory extends EdgeVersionFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jEdgeVersionFactory.class);
  private final Neo4jClient dbClient;
  private final IdGenerator idGenerator;

  private final Neo4jEdgeFactory edgeFactory;
  private final Neo4jRichVersionFactory richVersionFactory;

  public Neo4jEdgeVersionFactory(Neo4jEdgeFactory edgeFactory, Neo4jRichVersionFactory richVersionFactory, Neo4jClient dbClient, IdGenerator idGenerator) {
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
                            long fromId,
                            long toId,
                            List<Long> parentIds) throws GroundDBException {

    try {
      long id = idGenerator.generateVersionId();

      tags = tags.values().stream().collect(Collectors.toMap(Tag::getKey, tag -> new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType())));

      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("id", GroundType.LONG, id));
      insertions.add(new DbDataContainer("edge_id", GroundType.LONG, edgeId));
      insertions.add(new DbDataContainer("endpoint_one", GroundType.LONG, fromId));
      insertions.add(new DbDataContainer("endpoint_two", GroundType.LONG, toId));

      this.dbClient.addVertex("EdgeVersion", insertions);
      this.richVersionFactory.insertIntoDatabase(id, tags, structureVersionId, reference, referenceParameters);

      this.dbClient.addEdge("EdgeVersionConnection", fromId, id, new ArrayList<>());
      this.dbClient.addEdge("EdgeVersionConnection", id, toId, new ArrayList<>());

      this.edgeFactory.update(edgeId, id, parentIds);

      this.dbClient.commit();
      LOGGER.info("Created edge version " + id + " in edge " + edgeId + ".");

      return EdgeVersionFactory.construct(id, tags, structureVersionId, reference, referenceParameters, edgeId, fromId, toId);
    } catch (GroundDBException e) {
      this.dbClient.abort();
      throw e;
    }
  }

  public EdgeVersion retrieveFromDatabase(long id) throws GroundDBException {
    try {
      RichVersion version = this.richVersionFactory.retrieveFromDatabase(id);

      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer("id", GroundType.LONG, id));

      Record versionRecord;
      try {
        versionRecord = this.dbClient.getVertex(predicates);
      } catch (EmptyResultException e) {
        throw new GroundDBException("No EdgeVersion found with id " + id + ".");
      }

      long edgeId = versionRecord.get("v").asNode() .get("edge_id").asLong();
      long fromId = versionRecord.get("v").asNode().get("endpoint_one").asLong();
      long toId = versionRecord.get("v").asNode().get("endpoint_two").asLong();

      this.dbClient.commit();
      LOGGER.info("Retrieved edge version " + id + " in edge " + edgeId + ".");

      return EdgeVersionFactory.construct(id, version.getTags(), version.getStructureVersionId(), version.getReference(), version.getParameters(), edgeId, fromId, toId);
    } catch (GroundDBException e) {
      this.dbClient.abort();

      throw e;
    }
  }
}
