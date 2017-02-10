package edu.berkeley.ground.api.versions.cassandra;

import org.junit.Test;

import edu.berkeley.ground.api.CassandraTest;
import edu.berkeley.ground.api.versions.VersionHistoryDAG;
import edu.berkeley.ground.api.versions.VersionSuccessor;
import edu.berkeley.ground.db.CassandraClient.CassandraConnection;
import edu.berkeley.ground.exceptions.GroundDBException;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

// TODO: Close connections to avoid littering.
public class CassandraVersionHistoryDAGFactoryTest extends CassandraTest {

  public CassandraVersionHistoryDAGFactoryTest() throws GroundDBException {
    super();
  }

  @Test
  public void testVersionHistoryDAGCreation() throws GroundException {
    long testId = 1;
    super.versionHistoryDAGFactory.create(testId);
    CassandraConnection connection = super.cassandraClient.getConnection();

    VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(connection,
        testId);

    assertEquals(0, dag.getEdgeIds().size());

    connection.abort();
  }

  @Test
  public void testAddEdge() throws GroundException {
    long testId = 1;
    CassandraConnection connection = super.cassandraClient.getConnection();
    super.versionHistoryDAGFactory.create(testId);

    VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(connection,
        testId);

    long fromId = 123;
    long toId = 456;

    super.versionFactory.insertIntoDatabase(connection, fromId);
    super.versionFactory.insertIntoDatabase(connection, toId);

    super.versionHistoryDAGFactory.addEdge(connection, dag, fromId, toId, testId);

    VersionHistoryDAG<?> retrieved = super.versionHistoryDAGFactory.retrieveFromDatabase(connection,
        testId);

    assertEquals(1, retrieved.getEdgeIds().size());
    assertEquals(toId, (long) retrieved.getLeaves().get(0));

    VersionSuccessor<?> successor = super.versionSuccessorFactory.retrieveFromDatabase(
        connection, retrieved.getEdgeIds().get(0));

    assertEquals(fromId, successor.getFromId());
    assertEquals(toId, successor.getToId());
  }
}
