/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * <p>
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.ground.postgres.dao.usage;

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.common.dao.usage.LineageGraphVersionDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.core.RichVersion;
import edu.berkeley.ground.common.model.usage.LineageGraphVersion;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.core.PostgresRichVersionDao;
import edu.berkeley.ground.postgres.util.PostgresStatements;
import edu.berkeley.ground.postgres.util.PostgresUtils;
import java.util.ArrayList;
import java.util.List;
import play.db.Database;
import play.libs.Json;

public class PostgresLineageGraphVersionDao extends PostgresRichVersionDao<LineageGraphVersion> implements LineageGraphVersionDao {

  private PostgresLineageGraphDao postgresLineageGraphDao;

  public PostgresLineageGraphVersionDao(Database dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
    this.postgresLineageGraphDao = new PostgresLineageGraphDao(dbSource, idGenerator);
  }

  @Override
  public LineageGraphVersion create(LineageGraphVersion lineageGraphVersion, List<Long> parentIds)
    throws GroundException {
    final long uniqueId = idGenerator.generateVersionId();
    LineageGraphVersion newLineageGraphVersion = new LineageGraphVersion(uniqueId, lineageGraphVersion);

    PostgresStatements updateVersionList = this.postgresLineageGraphDao
                                             .update(newLineageGraphVersion.getLineageGraphId(), newLineageGraphVersion.getId(),
                                               parentIds);

    try {
      PostgresStatements statements = super.insert(newLineageGraphVersion);
      statements.append(String.format("insert into lineage_graph_version (id, lineage_graph_id) values (%d,%d)", uniqueId,
        newLineageGraphVersion.getLineageGraphId()));

      statements.merge(updateVersionList);

      for (Long id : newLineageGraphVersion.getLineageEdgeVersionIds()) {
        // TODO: Wtf is on conflict do nothing?
        statements.append(String.format(
          "insert into lineage_graph_version_edge (lineage_graph_version_id, lineage_edge_version_id) values (%d, %d) on conflict do nothing",
          newLineageGraphVersion.getLineageGraphId(), id));
      }

      PostgresUtils.executeSqlList(dbSource, statements);
    } catch (Exception e) {
      throw new GroundException(e);
    }

    return newLineageGraphVersion;
  }

  @Override
  public LineageGraphVersion retrieveFromDatabase(long id) throws GroundException {
    String sql = String.format("select * from lineage_graph_version where id=%d", id);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    if (json.size() == 0) {
      throw new GroundException(String.format("Lineage Graph Version with id %d does not exist.", id));
    }

    LineageGraphVersion lineageGraphVersion = Json.fromJson(json.get(0), LineageGraphVersion.class);

    // Get LineageEdgeIds
    List<Long> edgeIds = new ArrayList<>();
    sql = String.format("select * from lineage_graph_version_edge where lineage_graph_version_id=%d", id);

    JsonNode edgeJson = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    for (JsonNode edge : edgeJson) {
      edgeIds.add(edge.get("lineageEdgeVersionId").asLong());
    }

    RichVersion richVersion = super.retrieveFromDatabase(id);

    return new LineageGraphVersion(lineageGraphVersion.getId(), richVersion.getTags(),
                                    richVersion.getStructureVersionId(),
                                    richVersion.getReference(), richVersion.getParameters(),
                                    lineageGraphVersion.getLineageGraphId(), edgeIds);
  }
}
