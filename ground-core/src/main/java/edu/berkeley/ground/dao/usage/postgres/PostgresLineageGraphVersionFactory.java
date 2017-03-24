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

package edu.berkeley.ground.dao.usage.postgres;

import edu.berkeley.ground.model.models.RichVersion;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.dao.models.postgres.PostgresRichVersionFactory;
import edu.berkeley.ground.model.usage.LineageGraphVersion;
import edu.berkeley.ground.dao.usage.LineageGraphVersionFactory;
import edu.berkeley.ground.model.versions.GroundType;
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

public class PostgresLineageGraphVersionFactory extends LineageGraphVersionFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresLineageGraphVersionFactory
      .class);

  private final PostgresClient dbClient;
  private final PostgresLineageGraphFactory lineageGraphFactory;
  private final PostgresRichVersionFactory richVersionFactory;

  private final IdGenerator idGenerator;

  public PostgresLineageGraphVersionFactory(PostgresLineageGraphFactory lineageGraphFactory,
                                            PostgresRichVersionFactory richVersionFactory,
                                            PostgresClient dbClient,
                                            IdGenerator idGenerator) {
    this.dbClient = dbClient;
    this.lineageGraphFactory = lineageGraphFactory;
    this.richVersionFactory = richVersionFactory;
    this.idGenerator = idGenerator;
  }

  public LineageGraphVersion create(Map<String, Tag> tags,
                                    long structureVersionId,
                                    String reference,
                                    Map<String, String> referenceParameters,
                                    long lineageGraphId,
                                    List<Long> lineageEdgeVersionIds,
                                    List<Long> parentIds) throws GroundException {

    try {
      long id = this.idGenerator.generateVersionId();

      tags = tags.values().stream().collect(Collectors.toMap(Tag::getKey, tag -> new Tag(id, tag
          .getKey(), tag.getValue(), tag.getValueType())));

      this.richVersionFactory.insertIntoDatabase(id, tags, structureVersionId, reference,
          referenceParameters);

      List<DbDataContainer> insertions = new ArrayList<>();
      insertions.add(new DbDataContainer("id", GroundType.LONG, id));
      insertions.add(new DbDataContainer("lineage_graph_id", GroundType.LONG, lineageGraphId));

      this.dbClient.insert("lineage_graph_version", insertions);

      for (long lineageEdgeVersionId : lineageEdgeVersionIds) {
        List<DbDataContainer> lineageEdgeInsertion = new ArrayList<>();
        lineageEdgeInsertion.add(new DbDataContainer("lineage_graph_version_id", GroundType.LONG, id));
        lineageEdgeInsertion.add(new DbDataContainer("lineage_edge_version_id", GroundType.LONG,
            lineageEdgeVersionId));

        this.dbClient.insert("lineage_graph_version_edge", lineageEdgeInsertion);
      }

      this.lineageGraphFactory.update(lineageGraphId, id, parentIds);

      this.dbClient.commit();
      LOGGER.info("Created lineage_graph version " + id + " in lineage_graph " + lineageGraphId + ".");

      return LineageGraphVersionFactory.construct(id, tags, structureVersionId, reference,
          referenceParameters, lineageGraphId, lineageEdgeVersionIds);
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }
  }

  public LineageGraphVersion retrieveFromDatabase(long id) throws GroundException {
    try {
      RichVersion version = this.richVersionFactory.retrieveFromDatabase(id);

      List<DbDataContainer> predicates = new ArrayList<>();
      predicates.add(new DbDataContainer("id", GroundType.LONG, id));

      List<DbDataContainer> lineageEdgePredicate = new ArrayList<>();
      lineageEdgePredicate.add(new DbDataContainer("lineage_graph_version_id", GroundType.LONG, id));

      QueryResults resultSet;
      try {
        resultSet = this.dbClient.equalitySelect("lineage_graph_version", DBClient.SELECT_STAR, predicates);
      } catch (EmptyResultException e) {
        throw new GroundException("No LineageGraphVersion found with id " + id + ".");
      }

      long lineageGraphId = resultSet.getLong(2);

      QueryResults lineageEdgeSet;
      List<Long> lineageEdgeVersionIds = new ArrayList<>();
      try {
        lineageEdgeSet = this.dbClient.equalitySelect("lineage_graph_version_edge", DBClient
            .SELECT_STAR, lineageEdgePredicate);

        do {
          lineageEdgeVersionIds.add(lineageEdgeSet.getLong(2));
        } while (lineageEdgeSet.next());
      } catch (EmptyResultException e) {
        // do nothing; this just means that there are no edges in the LineageGraphVersion
      }

      this.dbClient.commit();
      LOGGER.info("Retrieved lineage_graph version "
                  + id
                  + " in lineage_graph "
                  + lineageGraphId + ".");

      return LineageGraphVersionFactory.construct(id, version.getTags(), version
          .getStructureVersionId(), version.getReference(), version.getParameters(),
          lineageGraphId, lineageEdgeVersionIds);
    } catch (GroundException e) {
      this.dbClient.abort();

      throw e;
    }
  }
}
