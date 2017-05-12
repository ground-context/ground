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

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.factory.usage.LineageGraphVersionFactory;
import edu.berkeley.ground.common.model.usage.LineageGraphVersion;
import edu.berkeley.ground.common.utils.IdGenerator;
import edu.berkeley.ground.postgres.dao.core.RichVersionDao;
import edu.berkeley.ground.postgres.utils.PostgresStatements;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import play.db.Database;

import java.util.ArrayList;
import java.util.List;

public class LineageGraphVersionDao extends RichVersionDao<LineageGraphVersion> implements LineageGraphVersionFactory {

  @Override
  public LineageGraphVersion create(LineageGraphVersion lineageGraphVersion) throws GroundException {
    PostgresStatements statements = super.insert(lineageGraphVersion);
    statements.append(String.format("insert into lineage_graph_version (id, lineage_graph_id) values (%d, %d)",
      lineageGraphVersion.getId(), lineageGraphVersion.getLineageGraphId()));
    try {
      PostgresUtils.executeSqlList(dbSource, statements);
      return lineageGraphVersion;
    } catch (Exception e) {
      throw new GroundException(e);
    }
  }
}
