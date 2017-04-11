/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.dao.versions.postgres;

import edu.berkeley.ground.dao.versions.VersionFactory;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.db.PostgresResults;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.exceptions.GroundVersionNotFoundException;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.model.versions.Version;

import java.util.ArrayList;
import java.util.List;

public abstract class PostgresVersionFactory<T extends Version> implements VersionFactory<T> {
  private final PostgresClient dbClient;

  public PostgresVersionFactory(PostgresClient dbClient) {
    this.dbClient = dbClient;
  }

  /**
   * Insert version information into the database.
   *
   * @param id the id to insert
   * @throws GroundException the id already exists in the database
   */
  @Override
  public void insertIntoDatabase(long id) throws GroundException {
    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("id", GroundType.LONG, id));

    this.dbClient.insert("version", insertions);
  }


  /**
   * Verify that a result set for a version is not empty.
   *
   * @param resultSet the result set to check
   * @param id the id of the version
   * @throws GroundVersionNotFoundException an exception indicating the item wasn't found
   */
  protected void verifyResultSet(PostgresResults resultSet, long id)
      throws GroundException {

    if (resultSet.isEmpty()) {
      throw new GroundVersionNotFoundException(this.getType(), id);
    }
  }
}
