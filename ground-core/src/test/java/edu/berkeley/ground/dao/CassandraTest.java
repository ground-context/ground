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

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;

import edu.berkeley.ground.dao.models.cassandra.CassandraRichVersionFactory;
import edu.berkeley.ground.dao.models.cassandra.CassandraStructureVersionFactory;
import edu.berkeley.ground.dao.models.cassandra.CassandraTagFactory;
import edu.berkeley.ground.dao.versions.cassandra.CassandraItemFactory;
import edu.berkeley.ground.dao.versions.cassandra.CassandraVersionFactory;
import edu.berkeley.ground.dao.versions.cassandra.CassandraVersionHistoryDAGFactory;
import edu.berkeley.ground.dao.versions.cassandra.CassandraVersionSuccessorFactory;
import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.exceptions.GroundDBException;
import edu.berkeley.ground.util.CassandraFactories;
import edu.berkeley.ground.util.IdGenerator;

public class CassandraTest {
  private static String TEST_DB_NAME = "test";

  protected static CassandraClient cassandraClient;
  protected static CassandraFactories factories;
  protected static CassandraVersionFactory versionFactory;
  protected static CassandraVersionSuccessorFactory versionSuccessorFactory;
  protected static CassandraVersionHistoryDAGFactory versionHistoryDAGFactory;
  protected static CassandraItemFactory itemFactory;
  protected static CassandraRichVersionFactory richVersionFactory;
  protected static CassandraTagFactory tagFactory;

  @BeforeClass
  public static void setup() throws GroundDBException {
    cassandraClient = new CassandraClient("localhost", 9160, "test", "test", "");
    factories = new CassandraFactories(cassandraClient, 0, 1);

    versionFactory = new CassandraVersionFactory(cassandraClient);
    versionSuccessorFactory = new CassandraVersionSuccessorFactory(cassandraClient, new IdGenerator(0, 1, false));
    versionHistoryDAGFactory = new CassandraVersionHistoryDAGFactory(cassandraClient, versionSuccessorFactory);
    tagFactory = new CassandraTagFactory(cassandraClient);
    itemFactory = new CassandraItemFactory(cassandraClient, versionHistoryDAGFactory, tagFactory);

    richVersionFactory = new CassandraRichVersionFactory(cassandraClient, versionFactory,
        (CassandraStructureVersionFactory) factories.getStructureVersionFactory(), tagFactory);
  }

  @Before
  public void setupTest() throws IOException, InterruptedException {
    Process p = Runtime.getRuntime().exec("cqlsh -k " + TEST_DB_NAME + " -f truncate.cql", null, new File("scripts/cassandra/"));
    p.waitFor();

    p.destroy();
  }

  @AfterClass
  public static void tearDown() throws IOException, InterruptedException {
    cassandraClient.close();
  }
}
