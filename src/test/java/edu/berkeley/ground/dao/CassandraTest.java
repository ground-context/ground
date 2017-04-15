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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.Session;
import com.typesafe.config.Config;
import edu.berkeley.ground.util.TestEnv;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import edu.berkeley.ground.dao.models.cassandra.CassandraStructureVersionFactory;
import edu.berkeley.ground.dao.models.cassandra.CassandraTagFactory;
import edu.berkeley.ground.dao.versions.cassandra.CassandraVersionHistoryDagFactory;
import edu.berkeley.ground.dao.versions.cassandra.CassandraVersionSuccessorFactory;
import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.exceptions.GroundDbException;
import edu.berkeley.ground.resources.EdgesResource;
import edu.berkeley.ground.resources.GraphsResource;
import edu.berkeley.ground.resources.LineageEdgesResource;
import edu.berkeley.ground.resources.LineageGraphsResource;
import edu.berkeley.ground.resources.NodesResource;
import edu.berkeley.ground.resources.StructuresResource;
import edu.berkeley.ground.util.CassandraFactories;
import edu.berkeley.ground.util.IdGenerator;

public class CassandraTest extends DaoTest {

  private static String TRUNCATE_SCRIPT = "./scripts/cassandra/truncate.cql";
  private static String CREATE_SCHEMA_SCRIPT = "./scripts/cassandra/cassandra.cql";

  private static final Function<String,String> CREATE_KEYSPACE_CQL = keyspace-> "create keyspace IF NOT EXISTS "+
    keyspace + " with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";

  private static CassandraFactories factories;

  protected static CassandraClient cassandraClient;
  protected static CassandraVersionSuccessorFactory versionSuccessorFactory;
  protected static CassandraVersionHistoryDagFactory versionHistoryDAGFactory;
  protected static CassandraTagFactory tagFactory;

  @BeforeClass
  public static void setup() throws GroundDbException {
    cassandraClient = setupCassandraClient();

    runScript(CREATE_SCHEMA_SCRIPT);

    factories = new CassandraFactories(cassandraClient, 0, 1);

    versionSuccessorFactory = new CassandraVersionSuccessorFactory(cassandraClient,
        new IdGenerator(0, 1, false));
    versionHistoryDAGFactory = new CassandraVersionHistoryDagFactory(cassandraClient,
        versionSuccessorFactory);
    tagFactory = new CassandraTagFactory(cassandraClient);

    edgesResource = new EdgesResource(factories.getEdgeFactory(),
        factories.getEdgeVersionFactory(), factories.getNodeFactory(), cassandraClient);
    graphsResource = new GraphsResource(factories.getGraphFactory(),
        factories.getGraphVersionFactory(), cassandraClient);
    lineageEdgesResource = new LineageEdgesResource(factories.getLineageEdgeFactory(),
        factories.getLineageEdgeVersionFactory(), cassandraClient);
    lineageGraphsResource = new LineageGraphsResource(factories.getLineageGraphFactory(),
        factories.getLineageGraphVersionFactory(), cassandraClient);
    nodesResource = new NodesResource(factories.getNodeFactory(),
        factories.getNodeVersionFactory(), cassandraClient);
    structuresResource = new StructuresResource(factories.getStructureFactory(),
        factories.getStructureVersionFactory(), cassandraClient);
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

  protected static CassandraClient setupCassandraClient() {
    Config cassandraConfig = TestEnv.config.getConfig("cassandra");
    String host = cassandraConfig.getString("host");
    int port = cassandraConfig.getInt("port");
    String keyspace = cassandraConfig.getString("keyspace");
    String username = cassandraConfig.getString("username");
    String password = cassandraConfig.getString("password");

    Cluster cluster =
      Cluster.builder()
        .addContactPoint(host)
        .withPort(port)
        .withAuthProvider(new PlainTextAuthProvider(username, password))
        .build();
    Session baseSession = cluster.connect();
    baseSession.execute(CREATE_KEYSPACE_CQL.apply(keyspace));
    Session session = cluster.connect(keyspace);
    return new CassandraClient(cluster, session);
  }

  protected static void runScript(String scriptFile)  {
    final String CQL_COMMENT_START = "--";

    try (Stream<String> lines = Files.lines(Paths.get(scriptFile))) {
      String data = lines.filter(line -> !line.startsWith(CQL_COMMENT_START)).collect(Collectors.joining());
      Arrays.stream(data.split(";"))
        .map(chunk -> chunk + ";")
        .forEach(statement -> cassandraClient.getSession().execute(statement));
    }catch (IOException e) {
      throw new RuntimeException("Unable to read script file: "+ scriptFile);
    }
  }

}
