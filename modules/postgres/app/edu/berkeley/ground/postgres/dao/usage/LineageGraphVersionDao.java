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
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.factory.usage.LineageGraphVersionFactory;
import edu.berkeley.ground.common.model.core.RichVersion;
import edu.berkeley.ground.common.model.usage.LineageEdge;
import edu.berkeley.ground.common.model.usage.LineageGraphVersion;
import edu.berkeley.ground.common.utils.IdGenerator;
import edu.berkeley.ground.postgres.dao.core.RichVersionDao;
import edu.berkeley.ground.postgres.dao.version.ItemDao;
import edu.berkeley.ground.postgres.dao.version.TagDao;
import edu.berkeley.ground.postgres.dao.version.VersionHistoryDagDao;
import edu.berkeley.ground.postgres.dao.version.VersionSuccessorDao;
import edu.berkeley.ground.postgres.utils.PostgresStatements;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import play.db.Database;
import play.libs.Json;

import java.util.ArrayList;
import java.util.List;

public class LineageGraphVersionDao extends RichVersionDao<LineageGraphVersion> implements LineageGraphVersionFactory {

  public LineageGraphVersionDao(Database dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
  }

  @Override
  public LineageGraphVersion create(LineageGraphVersion lineageGraphVersion, List<Long> parentIds) throws GroundException {
    final long uniqueId = idGenerator.generateVersionId();
    LineageGraphVersion newLineageGraphVersion = new LineageGraphVersion(uniqueId, lineageGraphVersion.getTags(), lineageGraphVersion.getStructureVersionId(),
      lineageGraphVersion.getReference(), lineageGraphVersion.getParameters(), lineageGraphVersion.getLineageGraphId(), lineageGraphVersion.getLineageEdgeVersionIds());

    VersionSuccessorDao versionSuccessorDao = new VersionSuccessorDao(dbSource, idGenerator);
    VersionHistoryDagDao versionHistoryDagDao = new VersionHistoryDagDao(dbSource, versionSuccessorDao);
    TagDao tagDao = new TagDao(dbSource, idGenerator);

    //TODO: Ideally, I think this should add to the sqlList to support rollback???

    ItemDao itemDao = new ItemDao(dbSource, idGenerator, versionHistoryDagDao, tagDao);
    PostgresStatements updateVersionList = itemDao.update(newLineageGraphVersion.getLineageGraphId(), newLineageGraphVersion.getId(), parentIds);

    try {
      PostgresStatements statements = super.insert(newLineageGraphVersion);
      statements.append(String.format(
        "insert into lineage_graph_version (id, lineage_graph_id) values (%d,%d)",
        uniqueId, newLineageGraphVersion.getLineageGraphId()));
      statements.merge(updateVersionList);

      for (Long id: newLineageGraphVersion.getLineageEdgeVersionIds()) {
        statements.append(String.format(
          "insert into lineage_graph_version_edge (lineage_graph_version_id, lineage_edge_version_id) values (%d, %d)",
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

    return new LineageGraphVersion(lineageGraphVersion.getId(), richVersion.getTags(), richVersion.getStructureVersionId(),
      richVersion.getReference(), richVersion.getParameters(), lineageGraphVersion.getLineageGraphId(), edgeIds);
  }
}
