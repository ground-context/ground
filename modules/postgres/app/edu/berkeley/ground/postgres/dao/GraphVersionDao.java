/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.ground.postgres.dao;

import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.model.core.GraphVersion;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import java.util.ArrayList;
import java.util.List;
import play.db.Database;

public class GraphVersionDao {

  public final void create(final Database dbSource, final GraphVersion graphVersion)
      throws GroundException {
    final List<String> sqlList = new ArrayList<>();
    sqlList.add(
        String.format(
            "insert into graph_version (id, graph_id) values (%d, %d)",
            graphVersion.getId(), graphVersion.getGraphId()));
    PostgresUtils.executeSqlList(dbSource, sqlList);
  }
}
