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

import java.util.ArrayList;
import java.util.List;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.factory.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.common.model.usage.LineageEdgeVersion;
import edu.berkeley.ground.common.utils.IdGenerator;
import edu.berkeley.ground.postgres.dao.core.RichVersionDao;
import edu.berkeley.ground.postgres.utils.PostgresStatements;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import play.db.Database;

public class LineageEdgeVersionDao extends RichVersionDao<LineageEdgeVersion> implements LineageEdgeVersionFactory {

  @Override
  public LineageEdgeVersion create(final LineageEdgeVersion lineageEdgeVersion) throws GroundException {
    PostgresStatements statements = super.insert(lineageEdgeVersion);
    statements.append(String.format("insert into lineage_edge_version (id, lineage_edge_id, from_rich_version_id," +
        "to_rich_version_id) values (%d, %d, %d, %d)", lineageEdgeVersion.getId(),
      lineageEdgeVersion.getLineageEdgeId(), lineageEdgeVersion.getFromId(), lineageEdgeVersion.getToId()));
    try {
      PostgresUtils.executeSqlList(dbSource, statements);
      return lineageEdgeVersion;
    } catch (Exception e) {
      throw new GroundException(e);
    }
  }
}
