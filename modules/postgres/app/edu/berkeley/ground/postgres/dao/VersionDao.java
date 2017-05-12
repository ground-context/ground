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

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.factory.version.VersionFactory;
import edu.berkeley.ground.common.model.version.Version;
import edu.berkeley.ground.common.model.core.NodeVersion;
import edu.berkeley.ground.common.utils.IdGenerator;
import edu.berkeley.ground.postgres.utils.PostgresStatements;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import java.util.ArrayList;
import java.util.List;
import play.db.Database;

public class VersionDao<T extends Version> implements VersionFactory<T> {

  @Override
  public PostgresStatements insert(T version) throws GroundException {
    PostgresStatements statements = new PostgresStatements();
    statements.append(
      String.format(
        "insert into version (id) values (%d)",
        version.getId()));
    return statements;
  }

  public Version create(final Database dbSource, final T version, final IdGenerator idGenerator) throws GroundException {
    PostgresUtils.executeSqlList(dbSource, insert(version));
    return version;
  }

  @Override
  public T retrieveFromDatabase(Database dbSource, long id) throws GroundException {
    return null;
  }

}
