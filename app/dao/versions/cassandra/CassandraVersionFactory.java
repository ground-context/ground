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

package dao.versions.cassandra;

import dao.versions.VersionFactory;
import db.CassandraClient;
import db.DbEqualsCondition;
import exceptions.GroundException;
import models.versions.GroundType;
import models.versions.Version;

import java.util.ArrayList;
import java.util.List;

public abstract class CassandraVersionFactory<T extends Version> implements VersionFactory<T> {
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
  @Override
  public void insertIntoDatabase(long id) throws GroundException {
    List<DbEqualsCondition> insertions = new ArrayList<>();
    insertions.add(new DbEqualsCondition("id", GroundType.LONG, id));

    this.dbClient.insert("version", insertions);
  }
}
