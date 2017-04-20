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

package dao;

import java.util.HashMap;
import java.util.Map;
import play.Configuration;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.function.Function;

import dao.models.cassandra.CassandraStructureVersionFactory;
import dao.models.cassandra.CassandraTagFactory;
import dao.versions.cassandra.CassandraVersionHistoryDagFactory;
import dao.versions.cassandra.CassandraVersionSuccessorFactory;
import db.CassandraClient;
import exceptions.GroundDbException;
import util.CassandraFactories;
import util.IdGenerator;

public class CassandraTest extends DaoTest {

  private static final String TRUNCATE_SCRIPT = "./scripts/cassandra/truncate.cql";
  private static final String CREATE_SCHEMA_SCRIPT = "./scripts/cassandra/cassandra.cql";

  private static final Function<String,String> CREATE_KEYSPACE_CQL = keyspace-> "create keyspace IF NOT EXISTS "+
    keyspace + " with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";

  private static CassandraFactories factories;

  protected static CassandraClient cassandraClient;
  protected static CassandraVersionSuccessorFactory versionSuccessorFactory;
  protected static CassandraVersionHistoryDagFactory versionHistoryDAGFactory;
  protected static CassandraTagFactory tagFactory;

  @BeforeClass
  public static void setup() throws GroundDbException {
    factories = new CassandraFactories(createTestConfig());
    cassandraClient = (CassandraClient) factories.getDbClient();

    runScript(CREATE_SCHEMA_SCRIPT);

    versionSuccessorFactory = new CassandraVersionSuccessorFactory(cassandraClient,
        new IdGenerator(0, 1, false));
    versionHistoryDAGFactory = new CassandraVersionHistoryDagFactory(cassandraClient,
        versionSuccessorFactory);
    tagFactory = new CassandraTagFactory(cassandraClient);

    edgeFactory = factories.getEdgeFactory();
    graphFactory = factories.getGraphFactory();
    lineageEdgeFactory = factories.getLineageEdgeFactory();
    lineageGraphFactory = factories.getLineageGraphFactory();
    nodeFactory = factories.getNodeFactory();
    structureFactory = factories.getStructureFactory();

    edgeVersionFactory = factories.getEdgeVersionFactory();
    graphVersionFactory = factories.getGraphVersionFactory();
    lineageEdgeVersionFactory = factories.getLineageEdgeVersionFactory();
    lineageGraphVersionFactory = factories.getLineageGraphVersionFactory();
    nodeVersionFactory = factories.getNodeVersionFactory();
    structureVersionFactory = factories.getStructureVersionFactory();
  }

  @Before
  public void setupTest() {
    runScript(TRUNCATE_SCRIPT);
  }

  public static CassandraStructureVersionFactory getStructureVersionFactory() {
    return (CassandraStructureVersionFactory) CassandraTest.factories.getStructureVersionFactory();
  }

  @AfterClass
  public static void tearDown() throws IOException, InterruptedException {
    cassandraClient.close();
  }

  private static Configuration createTestConfig() {
    Map<String, Object> confMap = new HashMap<>();
    Map<String, Object> dbMap = new HashMap<>();
    Map<String, Object> machineMap = new HashMap<>();

    dbMap.put("host", "localhost");
    dbMap.put("port", 9042);
    dbMap.put("name", "test");
    dbMap.put("user", "test");
    dbMap.put("password", "");

    machineMap.put("id", 0);
    machineMap.put("count", 1);

    confMap.put("db", dbMap);
    confMap.put("machine", machineMap);

    return new Configuration(confMap);
  }

  protected static void runScript(String script) {
    DaoTest.runScript(script, cassandraClient.getSession()::execute);
  }
}
