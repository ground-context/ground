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

package edu.berkeley.ground.dao.versions.cassandra;

import edu.berkeley.ground.dao.versions.VersionFactory;
import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.versions.GroundType;

import java.util.ArrayList;
import java.util.List;

public class CassandraVersionFactory extends VersionFactory {
  private final CassandraClient dbClient;

  public CassandraVersionFactory(CassandraClient dbClient) {
    this.dbClient = dbClient;
  }

  /**
   * Insert version information into the database.
   *
   * @param id the id to insert
   * @throws GroundException the id already exists in the database
   */
  public void insertIntoDatabase(long id) throws GroundException {
    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("id", GroundType.LONG, id));

    this.dbClient.insert("version", insertions);
  }
}
