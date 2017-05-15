package edu.berkeley.ground.postgres.dao;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.utils.IdGenerator;
import edu.berkeley.ground.postgres.dao.core.*;
import edu.berkeley.ground.postgres.dao.usage.LineageEdgeDao;
import edu.berkeley.ground.postgres.dao.usage.LineageEdgeVersionDao;
import edu.berkeley.ground.postgres.dao.usage.LineageGraphDao;
import edu.berkeley.ground.postgres.dao.usage.LineageGraphVersionDao;
import edu.berkeley.ground.postgres.dao.version.TagDao;
import edu.berkeley.ground.postgres.dao.version.VersionHistoryDagDao;
import edu.berkeley.ground.postgres.dao.version.VersionSuccessorDao;
import edu.berkeley.ground.postgres.utils.Daos;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import play.Configuration;
import play.db.Database;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;


public class DaoTest {
  private static final String DROP_SCRIPT = "./scripts/postgres/drop_postgres.sql";
  private static final String CREATE_SCHEMA_SCRIPT = "./scripts/postgres/postgres.sql";

  //protected static Client postgresClient;
  protected static Daos daos;
  protected static Database dbSource;
  protected static IdGenerator idGenerator;
  protected static VersionSuccessorDao versionSuccessorDao;
  protected static VersionHistoryDagDao versionHistoryDagDao;
  protected static TagDao tagDao;
  protected static EdgeDao edgeDao;
  protected static GraphDao graphDao;
  protected static LineageEdgeDao lineageEdgeDao;
  protected static LineageGraphDao lineageGraphDao;
  protected static NodeDao nodeDao;
  protected static StructureDao structureDao;
  protected static EdgeVersionDao edgeVersionDao;
  protected static GraphVersionDao graphVersionDao;
  protected static LineageEdgeVersionDao lineageEdgeVersionDao;
  protected static LineageGraphVersionDao lineageGraphVersionDao;
  protected static NodeVersionDao nodeVersionDao;
  protected static StructureVersionDao structureVersionDao;

  public DaoTest(Database dbSource, IdGenerator idGenerator) {
    this.dbSource = dbSource;
    this.idGenerator = idGenerator;
  }

  @BeforeClass
  public static void setupClass() throws GroundException {

    versionSuccessorDao = new VersionSuccessorDao(dbSource, idGenerator);
    versionHistoryDagDao= new VersionHistoryDagDao(dbSource, versionSuccessorDao);
    tagDao = new TagDao();

    edgeDao = daos.getEdgeDao();
    graphDao = daos.getGraphDao();
    lineageEdgeDao = daos.getLineageEdgeDao();
    lineageGraphDao = daos.getLineageGraphDao();
    nodeDao = daos.getNodeDao();
    structureDao = daos.getStructureDao();

    edgeVersionDao = daos.getEdgeVersionDao();
    graphVersionDao = daos.getGraphVersionDao();
    lineageEdgeVersionDao = daos.getLineageEdgeVersionDao();
    lineageGraphVersionDao = daos.getLineageGraphVersionDao();
    nodeVersionDao = daos.getNodeVersionDao();
    structureVersionDao = daos.getStructureVersionDao();
  }

  @AfterClass
  public static void teardownClass() throws SQLException {
    dbSource.getConnection().close();
  }

  /*
  public static StructureVersionDao getStructureVersionDao() {
    return (StructureVersionDao) Test.factories.getStructureVersionDao();
  }

  @Before
  public void setup() throws IOException, InterruptedException, GroundException {
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
  } */

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
