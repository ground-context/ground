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

package edu.berkeley.ground.api.usage.neo4j;

import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.models.neo4j.Neo4jRichVersionFactory;
import edu.berkeley.ground.api.usage.LineageEdgeVersion;
import edu.berkeley.ground.api.usage.LineageEdgeVersionFactory;
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

public class Neo4jLineageEdgeVersionFactory extends LineageEdgeVersionFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jLineageEdgeVersionFactory.class);
  private Neo4jClient dbClient;
  private Neo4jLineageEdgeFactory lineageEdgeFactory;
  private Neo4jRichVersionFactory richVersionFactory;

  private IdGenerator idGenerator;

  public Neo4jLineageEdgeVersionFactory(Neo4jLineageEdgeFactory lineageEdgeFactory, Neo4jRichVersionFactory richVersionFactory, Neo4jClient dbClient, IdGenerator idGenerator) {
    this.dbClient = dbClient;
    this.lineageEdgeFactory = lineageEdgeFactory;
    this.richVersionFactory = richVersionFactory;
    this.idGenerator = idGenerator;
  }


  public LineageEdgeVersion create(Map<String, Tag> tags,
                                   long structureVersionId,
                                   String reference,
                                   Map<String, String> referenceParameters,
                                   long fromId,
                                   long toId,
                                   long lineageEdgeId,
                                   List<Long> parentIds) throws GroundException {

    Neo4jConnection connection = this.dbClient.getConnection();

    try {
      long id = this.idGenerator.generateVersionId();

      tags.values().stream().collect(Collectors.toMap(Tag::getKey, tag -> new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType())));

      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("id", GroundType.LONG, id));
      insertions.add(new DbDataContainer("lineageedge_id", GroundType.LONG, lineageEdgeId));
      insertions.add(new DbDataContainer("endpoint_one", GroundType.LONG, fromId));
      insertions.add(new DbDataContainer("endpoint_two", GroundType.LONG, toId));

      connection.addVertex("LineageEdgeVersions", insertions);

      this.lineageEdgeFactory.update(connection, lineageEdgeId, id, parentIds);
      this.richVersionFactory.insertIntoDatabase(connection, id, tags, structureVersionId, reference, referenceParameters);

      connection.addEdge("LineageEdgeVersionConnection", fromId, id, new ArrayList<>());
      connection.addEdge("LineageEdgeVersionConnection", id, toId, new ArrayList<>());

      connection.commit();
      LOGGER.info("Created lineage edge version " + id + " in lineage edge " + lineageEdgeId + ".");

      return LineageEdgeVersionFactory.construct(id, tags, structureVersionId, reference, referenceParameters, fromId, toId, lineageEdgeId);
    } catch (GroundException e) {
      connection.abort();

      throw e;
    }
  }

  public LineageEdgeVersion retrieveFromDatabase(long id) throws GroundException {
    Neo4jConnection connection = this.dbClient.getConnection();

    try {
      RichVersion version = this.richVersionFactory.retrieveFromDatabase(connection, id);

      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer("id", GroundType.LONG, id));

      Record versionRecord;
      try {
        versionRecord = connection.getVertex(predicates);
      } catch (EmptyResultException eer) {
        throw new GroundException("No LineageEdgeVersion found with id " + id + ".");
      }

      long lineageEdgeId = versionRecord. get("v").asNode().get("lineageedge_id").asLong();
      long fromId = versionRecord.get("v").asNode().get("endpoint_one").asLong();
      long toId = versionRecord.get("v").asNode().get("endpoint_two").asLong();

      connection.commit();
      LOGGER.info("Retrieved lineage edge version " + id + " in lineage edge " + lineageEdgeId + ".");

      return LineageEdgeVersionFactory.construct(id, version.getTags(), version.getStructureVersionId(), version.getReference(), version.getParameters(), fromId, toId, lineageEdgeId);
    } catch (GroundException e) {
      connection.abort();

      throw e;
    }
  }
}
