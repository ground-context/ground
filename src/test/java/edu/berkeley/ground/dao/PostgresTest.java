/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use file except in compliance with the License.
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

import com.typesafe.config.Config;
import edu.berkeley.ground.util.TestEnv;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import edu.berkeley.ground.dao.models.postgres.PostgresStructureVersionFactory;
import edu.berkeley.ground.dao.models.postgres.PostgresTagFactory;
import edu.berkeley.ground.dao.versions.postgres.PostgresVersionHistoryDagFactory;
import edu.berkeley.ground.dao.versions.postgres.PostgresVersionSuccessorFactory;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.exceptions.GroundDbException;
import edu.berkeley.ground.resources.EdgesResource;
import edu.berkeley.ground.resources.GraphsResource;
import edu.berkeley.ground.resources.LineageEdgesResource;
import edu.berkeley.ground.resources.LineageGraphsResource;
import edu.berkeley.ground.resources.NodesResource;
import edu.berkeley.ground.resources.StructuresResource;
import edu.berkeley.ground.util.IdGenerator;
import edu.berkeley.ground.util.PostgresFactories;
import org.junit.Test;

public class PostgresTest extends DaoTest {
  private static String DROP_SCRIPT = "./scripts/postgres/drop_postgres.sql";
  private static String CREATE_SCHEMA_SCRIPT = "./scripts/postgres/postgres.sql";

  private static PostgresFactories factories;

  protected static PostgresClient postgresClient;
  protected static PostgresVersionSuccessorFactory versionSuccessorFactory;
  protected static PostgresVersionHistoryDagFactory versionHistoryDAGFactory;
  protected static PostgresTagFactory tagFactory;

  @BeforeClass
  public static void setupClass() throws GroundDbException {
    postgresClient = setupClient();
    runScript(CREATE_SCHEMA_SCRIPT);
    factories = new PostgresFactories(postgresClient, 0, 1);

    versionSuccessorFactory = new PostgresVersionSuccessorFactory(postgresClient, new IdGenerator(0, 1, false));
    versionHistoryDAGFactory = new PostgresVersionHistoryDagFactory(postgresClient, versionSuccessorFactory);
    tagFactory = new PostgresTagFactory(postgresClient);

    edgesResource = new EdgesResource(factories.getEdgeFactory(),
        factories.getEdgeVersionFactory(), factories.getNodeFactory(), postgresClient);
    graphsResource = new GraphsResource(factories.getGraphFactory(),
        factories.getGraphVersionFactory(), postgresClient);
    lineageEdgesResource = new LineageEdgesResource(factories.getLineageEdgeFactory(),
        factories.getLineageEdgeVersionFactory(), postgresClient);
    lineageGraphsResource = new LineageGraphsResource(factories.getLineageGraphFactory(),
        factories.getLineageGraphVersionFactory(), postgresClient);
    nodesResource = new NodesResource(factories.getNodeFactory(),
        factories.getNodeVersionFactory(), postgresClient);
    structuresResource = new StructuresResource(factories.getStructureFactory(),
        factories.getStructureVersionFactory(), postgresClient);
  }

  @AfterClass
  public static void teardownClass() throws GroundDbException {
    // postgresClient.close();
  }

  public static PostgresStructureVersionFactory getStructureVersionFactory() {
    return (PostgresStructureVersionFactory) PostgresTest.factories.getStructureVersionFactory();
  }

  @Test
  public void dummy() {
    System.out.println("done!");
  }

  @Before
  public void setup() throws IOException, InterruptedException, GroundDbException {
    System.out.println("setup **************************************");
    long t0 = System.currentTimeMillis();
    runScript(DROP_SCRIPT);
    runScript(CREATE_SCHEMA_SCRIPT);
    System.out.println("setup took: "+(System.currentTimeMillis()-t0)+" ms.");
  }

  protected static PostgresClient setupClient() throws GroundDbException {
    Config postgresConfig = TestEnv.config.getConfig("postgres");
    String host = postgresConfig.getString("host");
    int port = postgresConfig.getInt("port");
    String dbName = postgresConfig.getString("dbName");
    String username = postgresConfig.getString("username");
    String password = postgresConfig.getString("password");
    return new PostgresClient(host, port, dbName, username, password);
  }

  protected static void runScript(String script)  {
    try {
      boolean autoCommitState = postgresClient.getConnection().getAutoCommit();
      postgresClient.getConnection().setAutoCommit(true);
      StatementExecutor exec = new StatementExecutor(postgresClient.getConnection());
      DaoTest.runScript(script, exec::execute);
      postgresClient.getConnection().setAutoCommit(autoCommitState);
    } catch (SQLException ex) {
      throw new RuntimeException(ex);
    }
  }

  private static class StatementExecutor {
    private final Connection conn;
    StatementExecutor(Connection conn) {
      this.conn = conn;
    }
    void execute(String statement)  {
      try {
        Statement sqlStatement = conn.createStatement();
        sqlStatement.execute(statement);
      } catch (SQLException e) {
        String message = e.getMessage();
        if (message.contains("already exists") || message.contains("current transaction is aborted")) {
          System.out.println("Warn: statement ["+statement+"] causes: "+e.getMessage());
          // ignore errors caused by type already existing
        } else {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
