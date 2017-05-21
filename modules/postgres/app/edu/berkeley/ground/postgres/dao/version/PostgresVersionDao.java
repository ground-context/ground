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
package edu.berkeley.ground.postgres.dao.version;

import edu.berkeley.ground.common.dao.version.VersionDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.version.Version;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.SqlConstants;
import edu.berkeley.ground.postgres.util.PostgresStatements;
import play.db.Database;

public abstract class PostgresVersionDao<T extends Version> implements VersionDao<T> {

  protected Database dbSource;
  protected IdGenerator idGenerator;

  public PostgresVersionDao(Database dbSource, IdGenerator idGenerator) {
    this.dbSource = dbSource;
    this.idGenerator = idGenerator;
  }

  @Override
  public PostgresStatements insert(T version) throws GroundException {
    PostgresStatements statements = new PostgresStatements();

    statements.append(String.format(SqlConstants.INSERT_VERSION, version.getId()));
    return statements;
  }

  @Override
  public PostgresStatements delete(long id) {
    PostgresStatements statements = new PostgresStatements();
    statements.append(String.format(SqlConstants.DELETE_BY_ID, "version", id));

    return statements;
  }
}
