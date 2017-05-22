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
import edu.berkeley.ground.common.dao.usage.LineageEdgeVersionDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.exception.GroundException.ExceptionType;
import edu.berkeley.ground.common.model.core.RichVersion;
import edu.berkeley.ground.common.model.usage.LineageEdgeVersion;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.SqlConstants;
import edu.berkeley.ground.postgres.dao.core.PostgresRichVersionDao;
import edu.berkeley.ground.postgres.util.PostgresStatements;
import edu.berkeley.ground.postgres.util.PostgresUtils;
import java.util.List;
import play.db.Database;
import play.libs.Json;

public class PostgresLineageEdgeVersionDao extends PostgresRichVersionDao<LineageEdgeVersion> implements LineageEdgeVersionDao {

  private PostgresLineageEdgeDao postgresLineageEdgeDao;

  public PostgresLineageEdgeVersionDao(Database dbSource, IdGenerator idGenerator) {
    super(dbSource, idGenerator);
    this.postgresLineageEdgeDao = new PostgresLineageEdgeDao(dbSource, idGenerator);
  }

  @Override
  public LineageEdgeVersion create(final LineageEdgeVersion lineageEdgeVersion,
                                    List<Long> parentIds) throws GroundException {
    final long uniqueId = idGenerator.generateVersionId();

    LineageEdgeVersion newLineageEdgeVersion = new LineageEdgeVersion(uniqueId, lineageEdgeVersion);

    PostgresStatements updateVersionList = this.postgresLineageEdgeDao
                                             .update(newLineageEdgeVersion.getLineageEdgeId(), newLineageEdgeVersion.getId(), parentIds);

    try {
      PostgresStatements statements = super.insert(newLineageEdgeVersion);
      statements.append(String.format(SqlConstants.INSERT_LINEAGE_EDGE_VERSION, uniqueId, newLineageEdgeVersion.getLineageEdgeId(),
        newLineageEdgeVersion.getFromId(), newLineageEdgeVersion.getToId(), null));
      statements.merge(updateVersionList);

      PostgresUtils.executeSqlList(dbSource, statements);
    } catch (Exception e) {
      throw new GroundException(e);
    }
    return newLineageEdgeVersion;
  }

  @Override
  public PostgresStatements delete(long id) {
    PostgresStatements statements = new PostgresStatements();
    statements.append(String.format(SqlConstants.DELETE_BY_ID, "lineage_edge_version", id));

    PostgresStatements superStatements = super.delete(id);
    superStatements.merge(statements);
    return superStatements;
  }


  @Override
  public LineageEdgeVersion retrieveFromDatabase(long id) throws GroundException {
    String sql = String.format(SqlConstants.SELECT_STAR_BY_ID, "lineage_edge_version", id);
    JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));

    if (json.size() == 0) {
      throw new GroundException(ExceptionType.VERSION_NOT_FOUND, this.getType().getSimpleName(), String.format("%d", id));
    }

    LineageEdgeVersion lineageEdgeVersion = Json.fromJson(json.get(0), LineageEdgeVersion.class);
    RichVersion richVersion = super.retrieveFromDatabase(id);

    return new LineageEdgeVersion(id, richVersion, lineageEdgeVersion);
  }
}
