package edu.berkeley.ground.postgres.dao;

import com.google.common.collect.ImmutableMap;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.core.PostgresEdgeDao;
import edu.berkeley.ground.postgres.dao.core.PostgresEdgeVersionDao;
import edu.berkeley.ground.postgres.dao.core.PostgresGraphDao;
import edu.berkeley.ground.postgres.dao.core.PostgresGraphVersionDao;
import edu.berkeley.ground.postgres.dao.core.PostgresNodeDao;
import edu.berkeley.ground.postgres.dao.core.PostgresNodeVersionDao;
import edu.berkeley.ground.postgres.dao.core.PostgresStructureDao;
import edu.berkeley.ground.postgres.dao.core.PostgresStructureVersionDao;
import edu.berkeley.ground.postgres.dao.usage.PostgresLineageEdgeDao;
import edu.berkeley.ground.postgres.dao.usage.PostgresLineageEdgeVersionDao;
import edu.berkeley.ground.postgres.dao.usage.PostgresLineageGraphDao;
import edu.berkeley.ground.postgres.dao.usage.PostgresLineageGraphVersionDao;
import edu.berkeley.ground.postgres.dao.version.PostgresTagDao;
import edu.berkeley.ground.postgres.dao.version.PostgresVersionHistoryDagDao;
import edu.berkeley.ground.postgres.dao.version.PostgresVersionSuccessorDao;
import edu.berkeley.ground.postgres.dao.version.mock.TestPostgresItemDao;
import edu.berkeley.ground.postgres.dao.version.mock.TestPostgresRichVersionDao;
import edu.berkeley.ground.postgres.dao.version.mock.TestPostgresVersionDao;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.After;
import org.junit.Before;
import play.db.Database;
import play.db.Databases;


public class PostgresTest extends DaoTest {

  private static final String DROP_SCRIPT = "../../resources/scripts/postgres/drop_postgres.sql";
  private static final String CREATE_SCHEMA_SCRIPT = "../../resources/scripts/postgres/postgres.sql";

  public PostgresTest() throws GroundException {

  }

  @Before
  public void setup() throws IOException, InterruptedException, GroundException {
    Database dbSource = Databases.createFrom(
      "org.postgresql.Driver",
      "jdbc:postgresql://localhost:5432/test",
      ImmutableMap.of(
        "username", "test",
        "password", "test"
      ));

    IdGenerator idGenerator = new IdGenerator(0, 1, false);

    PostgresTest.dbSource = dbSource;
    PostgresTest.idGenerator = idGenerator;

    PostgresTest.postgresVersionDao = new TestPostgresVersionDao(dbSource, idGenerator);
    PostgresTest.versionSuccessorDao = new PostgresVersionSuccessorDao(dbSource, idGenerator);
    PostgresTest.versionHistoryDagDao = new PostgresVersionHistoryDagDao(dbSource, idGenerator);
    PostgresTest.postgresItemDao = new TestPostgresItemDao(dbSource, idGenerator);
    PostgresTest.tagDao = new PostgresTagDao(dbSource);

    PostgresTest.postgresRichVersionDao = new TestPostgresRichVersionDao(dbSource, idGenerator);
    PostgresTest.structureDao = new PostgresStructureDao(dbSource, idGenerator);
    PostgresTest.structureVersionDao = new PostgresStructureVersionDao(dbSource, idGenerator);
    PostgresTest.edgeDao = new PostgresEdgeDao(dbSource, idGenerator);
    PostgresTest.edgeVersionDao = new PostgresEdgeVersionDao(dbSource, idGenerator);
    PostgresTest.graphDao = new PostgresGraphDao(dbSource, idGenerator);
    PostgresTest.graphVersionDao = new PostgresGraphVersionDao(dbSource, idGenerator);
    PostgresTest.nodeDao = new PostgresNodeDao(dbSource, idGenerator);
    PostgresTest.nodeVersionDao = new PostgresNodeVersionDao(dbSource, idGenerator);
    PostgresTest.lineageEdgeDao = new PostgresLineageEdgeDao(dbSource, idGenerator);
    PostgresTest.lineageEdgeVersionDao = new PostgresLineageEdgeVersionDao(dbSource, idGenerator);
    PostgresTest.lineageGraphDao = new PostgresLineageGraphDao(dbSource, idGenerator);
    PostgresTest.lineageGraphVersionDao = new PostgresLineageGraphVersionDao(dbSource, idGenerator);

    runScript(DROP_SCRIPT);
    runScript(CREATE_SCHEMA_SCRIPT);
  }

  @After
  public void tearDown() throws IOException, InterruptedException, GroundException {
    dbSource.shutdown();
  }

  private static void runScript(String script) {
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

    void execute(String statement) {
      try {
        Statement sqlStatement = conn.createStatement();
        sqlStatement.execute(statement);
      } catch (SQLException e) {
        String message = e.getMessage();
        if (message.contains("already exists") || message
                                                    .contains("current transaction is aborted")) {
          System.out.println("Warn: statement [" + statement + "] causes: " + e.getMessage());
          // ignore errors caused by type already existing
        } else {
          throw new RuntimeException(e);
        }
      }
    }
  }

}
