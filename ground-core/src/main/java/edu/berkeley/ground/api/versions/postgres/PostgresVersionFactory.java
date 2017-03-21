/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.api.versions.postgres;

import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.api.versions.VersionFactory;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.ArrayList;
import java.util.List;

public class PostgresVersionFactory extends VersionFactory {
  private final PostgresClient dbClient;

  public PostgresVersionFactory(PostgresClient dbClient) {
    this.dbClient = dbClient;
  }

  public void insertIntoDatabase(long id) throws GroundException {
    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("id", GroundType.LONG, id));

    this.dbClient.insert("version", insertions);
  }
}
