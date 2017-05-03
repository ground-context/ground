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
import edu.berkeley.ground.lib.factory.version.VersionFactory;
import edu.berkeley.ground.lib.model.version.Version;
import edu.berkeley.ground.lib.model.core.NodeVersion;
import edu.berkeley.ground.postgres.utils.IdGenerator;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import java.util.ArrayList;
import java.util.List;
import play.db.Database;

public class VersionDao<T extends Version> implements VersionFactory<T> {

  @Override
  public void insertIntoDatabase(long id, T version) throws GroundException {
  }

  public void create(final Database dbSource, final T version, final IdGenerator idGenerator) throws GroundException {
    final List<String> sqlList = createSqlList(version);
    PostgresUtils.executeSqlList(dbSource, sqlList);
  }

  public List<String> createSqlList(final T version) throws GroundException {
    final List<String> sqlList = new ArrayList<>();
    sqlList.add(
      String.format(
        "insert into version (id) values (%d)",
        version.getId()));
      return sqlList;
  }

  @Override
  public T retrieveFromDatabase(Database dbSource, long id) throws GroundException {
    return null;
  }

  public final void create(final Database dbSource, final NodeVersion nodeVersion)
    throws GroundException {
    final List<String> sqlList = new ArrayList<>();
    sqlList.add(String.format("insert into version (id) values (%d)", nodeVersion.getId()));
    PostgresUtils.executeSqlList(dbSource, sqlList);
  }
}
