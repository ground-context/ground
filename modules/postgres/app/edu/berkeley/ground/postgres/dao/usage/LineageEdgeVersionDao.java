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
import edu.berkeley.ground.common.factory.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.common.model.core.RichVersion;
import edu.berkeley.ground.common.model.usage.LineageEdgeVersion;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.core.RichVersionDao;
import edu.berkeley.ground.postgres.util.PostgresStatements;
import edu.berkeley.ground.postgres.util.PostgresUtils;
import java.util.List;
import play.db.Database;
import play.libs.Json;

public class LineageEdgeVersionDao extends RichVersionDao<LineageEdgeVersion> implements LineageEdgeVersionFactory {

  private LineageEdgeDao lineageEdgeDao;

  public LineageEdgeVersionDao(Database dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
    this.lineageEdgeDao = new LineageEdgeDao(dbSource, idGenerator);
  }

  @Override
  public LineageEdgeVersion create(final LineageEdgeVersion lineageEdgeVersion,
                                    List<Long> parentIds) throws GroundException {
    final long uniqueId = idGenerator.generateVersionId();

    LineageEdgeVersion newLineageEdgeVersion = new LineageEdgeVersion(uniqueId, lineageEdgeVersion);

    PostgresStatements updateVersionList = this.lineageEdgeDao
                                             .update(newLineageEdgeVersion.getLineageEdgeId(), newLineageEdgeVersion.getId(), parentIds);

    try {
      PostgresStatements statements = super.insert(newLineageEdgeVersion);
      statements.append(String.format(
        "insert into lineage_edge_version (id, lineage_edge_id, from_rich_version_id, to_rich_version_id, principal_id) values (%d, %d, %d, %d, %d)",
        uniqueId, newLineageEdgeVersion.getLineageEdgeId(), newLineageEdgeVersion.getFromId(), newLineageEdgeVersion.getToId(), null));
      statements.merge(updateVersionList);

      PostgresUtils.executeSqlList(dbSource, statements);
    } catch (Exception e) {
      throw new GroundException(e);
    }
    return newLineageEdgeVersion;
  }

  @Override
  public LineageEdgeVersion retrieveFromDatabase(long id) throws GroundException {
    String sql = String.format("select * from lineage_edge_version where id=%d", id);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
    if (json.size() == 0) {
      throw new GroundException(String.format("Lineage Edge Version with id %d does not exist.", id));
    }

    LineageEdgeVersion lineageEdgeVersion = Json.fromJson(json.get(0), LineageEdgeVersion.class);
    RichVersion richVersion = super.retrieveFromDatabase(id);

    return new LineageEdgeVersion(id, richVersion.getTags(), richVersion.getStructureVersionId(), richVersion.getReference(),
                                   richVersion.getParameters(), lineageEdgeVersion.getFromId(), lineageEdgeVersion.getToId(),
                                   lineageEdgeVersion.getLineageEdgeId());
  }
}
