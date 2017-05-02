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

package dao;

import dao.models.postgres.PostgresStructureVersionFactory;
import dao.models.postgres.PostgresTagFactory;
import dao.versions.postgres.PostgresVersionHistoryDagFactory;
import dao.versions.postgres.PostgresVersionSuccessorFactory;
import db.PostgresClient;
import exceptions.GroundDbException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import play.Configuration;
import util.IdGenerator;
import util.PostgresFactories;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class PostgresTest extends DaoTest {
  private static final String DROP_SCRIPT = "./scripts/postgres/drop_postgres.sql";
  private static final String CREATE_SCHEMA_SCRIPT = "./scripts/postgres/postgres.sql";

  private static PostgresFactories factories;
  protected static PostgresClient postgresClient;
  protected static PostgresVersionSuccessorFactory versionSuccessorFactory;
  protected static PostgresVersionHistoryDagFactory versionHistoryDAGFactory;
  protected static PostgresTagFactory tagFactory;

  @BeforeClass
  public static void setupClass() throws GroundDbException {
    factories = new PostgresFactories(createTestConfig());
    postgresClient = (PostgresClient) factories.getDbClient();

    versionSuccessorFactory = new PostgresVersionSuccessorFactory(postgresClient, new IdGenerator(0, 1, false));
    versionHistoryDAGFactory = new PostgresVersionHistoryDagFactory(postgresClient, versionSuccessorFactory);
    tagFactory = new PostgresTagFactory(postgresClient, true);

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

  @AfterClass
  public static void teardownClass() throws GroundDbException {
    postgresClient.close();
  }

  public static PostgresStructureVersionFactory getStructureVersionFactory() {
    return (PostgresStructureVersionFactory) PostgresTest.factories.getStructureVersionFactory();
  }

  @Before
  public void setup() throws IOException, InterruptedException, GroundDbException {
    runScript(DROP_SCRIPT);
    runScript(CREATE_SCHEMA_SCRIPT);
  }

  private static void runScript(String script)  {
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

  private static Configuration createTestConfig() {
    Map<String, Object> confMap = new HashMap<>();
    Map<String, Object> dbMap = new HashMap<>();
    Map<String, Object> machineMap = new HashMap<>();

    dbMap.put("host", "localhost");
    dbMap.put("port", 5432);
    dbMap.put("name", "test");
    dbMap.put("user", "test");
    dbMap.put("password", "");

    machineMap.put("id", 0);
    machineMap.put("count", 1);

    confMap.put("db", dbMap);
    confMap.put("machine", machineMap) ;

    return new Configuration(confMap);
  }
}
