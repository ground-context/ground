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

package edu.berkeley.ground.dao;

import org.junit.Before;

import java.io.File;
import java.io.IOException;

import edu.berkeley.ground.dao.models.postgres.PostgresRichVersionFactory;
import edu.berkeley.ground.dao.models.postgres.PostgresStructureVersionFactory;
import edu.berkeley.ground.dao.models.postgres.PostgresTagFactory;
import edu.berkeley.ground.dao.versions.postgres.PostgresItemFactory;
import edu.berkeley.ground.dao.versions.postgres.PostgresVersionFactory;
import edu.berkeley.ground.dao.versions.postgres.PostgresVersionHistoryDagFactory;
import edu.berkeley.ground.dao.versions.postgres.PostgresVersionSuccessorFactory;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.exceptions.GroundDbException;
import edu.berkeley.ground.util.IdGenerator;
import edu.berkeley.ground.util.PostgresFactories;

public class PostgresTest {
  private static String TEST_DB_NAME = "test";

  protected PostgresClient postgresClient;
  protected PostgresFactories factories;
  protected PostgresVersionFactory versionFactory;
  protected PostgresVersionSuccessorFactory versionSuccessorFactory;
  protected PostgresVersionHistoryDagFactory versionHistoryDAGFactory;
  protected PostgresItemFactory itemFactory;
  protected PostgresRichVersionFactory richVersionFactory;
  protected PostgresTagFactory tagFactory;

  public PostgresTest() throws GroundDbException {
    this.postgresClient = new PostgresClient("localhost", 5432, "test", "test", "");
    this.factories = new PostgresFactories(this.postgresClient, 0, 1);

    this.versionFactory = new PostgresVersionFactory(this.postgresClient);
    this.versionSuccessorFactory = new PostgresVersionSuccessorFactory(this.postgresClient, new IdGenerator(0, 1, false));
    this.versionHistoryDAGFactory = new PostgresVersionHistoryDagFactory(this.postgresClient, versionSuccessorFactory);
    this.tagFactory = new PostgresTagFactory(this.postgresClient);
    this.itemFactory = new PostgresItemFactory(this.postgresClient, versionHistoryDAGFactory, tagFactory);

    this.richVersionFactory = new PostgresRichVersionFactory(this.postgresClient, versionFactory,
        (PostgresStructureVersionFactory) factories.getStructureVersionFactory(), tagFactory);
  }

  @Before
  public void setup() throws IOException, InterruptedException {
    Process p = Runtime.getRuntime().exec("python2.7 postgres_setup.py " + TEST_DB_NAME + " test drop"
        , null, new File("scripts/postgres/"));
    p.waitFor();

    p.destroy();
  }
}
