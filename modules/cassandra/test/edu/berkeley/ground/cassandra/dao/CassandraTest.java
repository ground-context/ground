package edu.berkeley.ground.cassandra.dao;

import com.google.common.collect.ImmutableMap;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.cassandra.dao.core.CassandraEdgeDao;
import edu.berkeley.ground.cassandra.dao.core.CassandraEdgeVersionDao;
import edu.berkeley.ground.cassandra.dao.core.CassandraGraphDao;
import edu.berkeley.ground.cassandra.dao.core.CassandraGraphVersionDao;
import edu.berkeley.ground.cassandra.dao.core.CassandraNodeDao;
import edu.berkeley.ground.cassandra.dao.core.CassandraNodeVersionDao;
import edu.berkeley.ground.cassandra.dao.core.CassandraStructureDao;
import edu.berkeley.ground.cassandra.dao.core.CassandraStructureVersionDao;
import edu.berkeley.ground.cassandra.dao.usage.CassandraLineageEdgeDao;
import edu.berkeley.ground.cassandra.dao.usage.CassandraLineageEdgeVersionDao;
import edu.berkeley.ground.cassandra.dao.usage.CassandraLineageGraphDao;
import edu.berkeley.ground.cassandra.dao.usage.CassandraLineageGraphVersionDao;
import edu.berkeley.ground.cassandra.dao.version.CassandraTagDao;
import edu.berkeley.ground.cassandra.dao.version.CassandraVersionHistoryDagDao;
import edu.berkeley.ground.cassandra.dao.version.CassandraVersionSuccessorDao;
import edu.berkeley.ground.cassandra.dao.version.mock.TestCassandraItemDao;
import edu.berkeley.ground.cassandra.dao.version.mock.TestCassandraRichVersionDao;
import edu.berkeley.ground.cassandra.dao.version.mock.TestCassandraVersionDao;
import edu.berkeley.ground.cassandra.util.CassandraDatabase;
import java.io.IOException;
import java.util.function.Function;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

// import java.sql.Connection;
// import java.sql.SQLException;
// import java.sql.Statement;
import org.junit.After;
import org.junit.Before;
// import play.db.Database;
// import play.db.Databases;
import play.Logger;


public class CassandraTest extends DaoTest {

  private static final String TRUNCATE_SCRIPT = "../../resources/scripts/cassandra/truncate.cql";
  private static final String CREATE_SCHEMA_SCRIPT = "../../resources/scripts/cassandra/cassandra.cql";
  private static final Function<String,String> CREATE_KEYSPACE_CQL = keyspace->"create keyspace IF NOT EXISTS " +
    keyspace + " with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"; // Andre - will this be used?

  public CassandraTest() throws GroundException {

  }

  @Before
  public void setup() throws IOException, InterruptedException, GroundException {
    CassandraDatabase dbSource = new CassandraDatabase(
      "localhost",
      9042,
      "test",
      "user",
      "password"
    );

    IdGenerator idGenerator = new IdGenerator(0, 1, false);

    CassandraTest.dbSource = dbSource;
    CassandraTest.idGenerator = idGenerator;

    CassandraTest.cassandraVersionDao = new TestCassandraVersionDao(dbSource, idGenerator);
    CassandraTest.versionSuccessorDao = new CassandraVersionSuccessorDao(dbSource, idGenerator);
    CassandraTest.versionHistoryDagDao = new CassandraVersionHistoryDagDao(dbSource, idGenerator);
    CassandraTest.cassandraItemDao = new TestCassandraItemDao(dbSource, idGenerator);
    CassandraTest.tagDao = new CassandraTagDao(dbSource);

    CassandraTest.cassandraRichVersionDao = new TestCassandraRichVersionDao(dbSource, idGenerator);
    CassandraTest.structureDao = new CassandraStructureDao(dbSource, idGenerator);
    CassandraTest.structureVersionDao = new CassandraStructureVersionDao(dbSource, idGenerator);
    CassandraTest.edgeDao = new CassandraEdgeDao(dbSource, idGenerator);
    CassandraTest.edgeVersionDao = new CassandraEdgeVersionDao(dbSource, idGenerator);
    CassandraTest.graphDao = new CassandraGraphDao(dbSource, idGenerator);
    CassandraTest.graphVersionDao = new CassandraGraphVersionDao(dbSource, idGenerator);
    CassandraTest.nodeDao = new CassandraNodeDao(dbSource, idGenerator);
    CassandraTest.nodeVersionDao = new CassandraNodeVersionDao(dbSource, idGenerator);
    CassandraTest.lineageEdgeDao = new CassandraLineageEdgeDao(dbSource, idGenerator);
    CassandraTest.lineageEdgeVersionDao = new CassandraLineageEdgeVersionDao(dbSource, idGenerator);
    CassandraTest.lineageGraphDao = new CassandraLineageGraphDao(dbSource, idGenerator);
    CassandraTest.lineageGraphVersionDao = new CassandraLineageGraphVersionDao(dbSource, idGenerator);

    runScript(TRUNCATE_SCRIPT);
    runScript(CREATE_SCHEMA_SCRIPT);
  }

  @After
  public void tearDown() throws IOException, InterruptedException, GroundException {
    dbSource.shutdown();
  }

  private static void runScript(String script) {
    // Can wrap around with try-catch for a QueryExecutionException if wanted (like in PostgresTest.java for Postgres Connector)
    Session session = dbSource.getSession();
    DaoTest.runScript(script, session::execute);
  }

  // private static void runScript(String script) { // Andre - What the heck is happening here
  //   try {
  //     boolean autoCommitState = dbSource.getConnection().getAutoCommit();
  //     dbSource.getConnection().setAutoCommit(false);
  //     StatementExecutor exec = new StatementExecutor(dbSource.getConnection());
  //     CassandraTest.runScript(script, exec::execute);
  //     dbSource.getConnection().setAutoCommit(autoCommitState);
  //   } catch (SQLException ex) {
  //     throw new RuntimeException(ex);
  //   }
  // }

  // private static class StatementExecutor {

  //   private final Connection conn;

  //   StatementExecutor(Connection conn) {
  //     this.conn = conn;
  //   }

  //   void execute(String statement) {
  //     try {
  //       Statement sqlStatement = conn.createStatement();
  //       sqlStatement.execute(statement);
  //     } catch (SQLException e) {
  //       String message = e.getMessage();
  //       if (message.contains("already exists") || message
  //                                                   .contains("current transaction is aborted")) {
  //         System.out.println("Warn: statement [" + statement + "] causes: " + e.getMessage());
  //         // ignore errors caused by type already existing
  //       } else {
  //         throw new RuntimeException(e);
  //       }
  //     }
  //   }
  // }

}
