package edu.berkeley.ground.postgres.dao;

import com.google.common.collect.ImmutableMap;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.utils.IdGenerator;
import edu.berkeley.ground.postgres.dao.version.TagDao;
import edu.berkeley.ground.postgres.dao.version.VersionHistoryDagDao;
import edu.berkeley.ground.postgres.dao.version.VersionSuccessorDao;
import edu.berkeley.ground.postgres.utils.Daos;
import javafx.geometry.Pos;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import play.db.Database;
import play.db.Databases;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;


public class PostgresTest extends DaoTest {
  private static final String DROP_SCRIPT = "./resources/drop_postgres.sql";
  private static final String CREATE_SCHEMA_SCRIPT = "./resources/postgres.sql";

  //protected static Client postgresClient;
  protected static Daos daos;

  public PostgresTest() throws GroundException {


  }

//  @BeforeClass
//  public static void setupClass() throws GroundException {
//  }

  @AfterClass
  public static void teardownClass() throws SQLException {
    //dbSource.getConnection().close();
  }

  @Before
  public void setup() throws IOException, InterruptedException, GroundException {
    Database dbSource = Databases.createFrom(
      "org.postgresql.Driver",
      "jdbc:postgresql://localhost:5432/ground",
      ImmutableMap.of(
        "user", "ground",
        "password", "metadata"
      ));
    IdGenerator idGenerator = new IdGenerator(0, 1, false);

    PostgresTest.dbSource = dbSource;
    PostgresTest.idGenerator = idGenerator;
    PostgresTest.daos = new Daos(dbSource, idGenerator);

    versionSuccessorDao = new VersionSuccessorDao(dbSource, idGenerator);
    versionHistoryDagDao= new VersionHistoryDagDao(dbSource, (VersionSuccessorDao) versionSuccessorDao);
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
    runScript(DROP_SCRIPT);
    runScript(CREATE_SCHEMA_SCRIPT);
  }

  @After
  public void tearDown() throws IOException, InterruptedException, GroundException {
    dbSource.shutdown();
  }

  private static void runScript(String script)  {
    try {
      boolean autoCommitState = dbSource.getConnection().getAutoCommit();
      dbSource.getConnection().setAutoCommit(false);
      StatementExecutor exec = new StatementExecutor(dbSource.getConnection());
      PostgresTest.runScript(script, exec::execute);
      dbSource.getConnection().setAutoCommit(autoCommitState);
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
