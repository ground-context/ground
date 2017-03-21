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

package edu.berkeley.ground.api.usage.postgres;

import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.models.postgres.PostgresRichVersionFactory;
import edu.berkeley.ground.api.usage.LineageEdgeVersion;
import edu.berkeley.ground.api.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.PostgresClient;
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

public class PostgresLineageEdgeVersionFactory extends LineageEdgeVersionFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresLineageEdgeVersionFactory.class);
  private final PostgresClient dbClient;
  private final PostgresLineageEdgeFactory lineageEdgeFactory;
  private final PostgresRichVersionFactory richVersionFactory;

  private final IdGenerator idGenerator;

  public PostgresLineageEdgeVersionFactory(PostgresLineageEdgeFactory lineageEdgeFactory, PostgresRichVersionFactory richVersionFactory, PostgresClient dbClient, IdGenerator idGenerator) {
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

    try {
      long id = this.idGenerator.generateVersionId();

      tags.values().stream().collect(Collectors.toMap(Tag::getKey, tag -> new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType())));

      this.richVersionFactory.insertIntoDatabase(id, tags, structureVersionId, reference, referenceParameters);

      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("id", GroundType.LONG, id));
      insertions.add(new DbDataContainer("lineage_edge_id", GroundType.LONG, lineageEdgeId));
      insertions.add(new DbDataContainer("from_rich_version_id", GroundType.LONG, fromId));
      insertions.add(new DbDataContainer("to_rich_version_id", GroundType.LONG, toId));

      this.dbClient.insert("lineage_edge_version", insertions);

      this.lineageEdgeFactory.update(lineageEdgeId, id, parentIds);

      this.dbClient.commit();
      LOGGER.info("Created lineage edge version " + id + " in lineage edge " + lineageEdgeId + ".");

      return LineageEdgeVersionFactory.construct(id, tags, structureVersionId, reference, referenceParameters, fromId, toId, lineageEdgeId);
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }
  }

  public LineageEdgeVersion retrieveFromDatabase(long id) throws GroundException {
    try {
      RichVersion version = this.richVersionFactory.retrieveFromDatabase(id);

      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer("id", GroundType.LONG, id));

      QueryResults resultSet;
      try {
        resultSet = this.dbClient.equalitySelect("lineage_edge_version", DBClient.SELECT_STAR, predicates);
      } catch (EmptyResultException e) {
        throw new GroundException("No LineageEdgeVersion found with id " + id + ".");
      }

      long lineageEdgeId = resultSet.getLong(2);
      long fromId = resultSet.getLong(3);
      long toId = resultSet.getLong(4);

      this.dbClient.commit();
      LOGGER.info("Retrieved lineage edge version " + id + " in lineage edge " + lineageEdgeId + ".");

      return LineageEdgeVersionFactory.construct(id, version.getTags(), version.getStructureVersionId(), version.getReference(), version.getParameters(), fromId, toId, lineageEdgeId);
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }
  }
}
