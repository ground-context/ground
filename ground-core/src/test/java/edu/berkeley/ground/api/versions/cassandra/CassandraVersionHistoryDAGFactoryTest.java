package edu.berkeley.ground.api.versions.cassandra;

import org.junit.Test;

import edu.berkeley.ground.api.CassandraTest;
import edu.berkeley.ground.api.versions.VersionHistoryDAG;
import edu.berkeley.ground.api.versions.VersionSuccessor;
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
    try {
      long testId = 1;
      super.versionHistoryDAGFactory.create(testId);

      VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(testId);

      assertEquals(0, dag.getEdgeIds().size());
    } finally {
      super.cassandraClient.abort();
    }
  }

  @Test
  public void testAddEdge() throws GroundException {
    try {
      long testId = 1;
      super.versionHistoryDAGFactory.create(testId);

      VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(testId);

      long fromId = 123;
      long toId = 456;

      super.versionFactory.insertIntoDatabase(fromId);
      super.versionFactory.insertIntoDatabase(toId);

      super.versionHistoryDAGFactory.addEdge(dag, fromId, toId, testId);

      VersionHistoryDAG<?> retrieved = super.versionHistoryDAGFactory.retrieveFromDatabase(testId);

      assertEquals(1, retrieved.getEdgeIds().size());
      assertEquals(toId, (long) retrieved.getLeaves().get(0));

      VersionSuccessor<?> successor = super.versionSuccessorFactory.retrieveFromDatabase(
          retrieved.getEdgeIds().get(0));

      assertEquals(fromId, successor.getFromId());
      assertEquals(toId, successor.getToId());
    } finally {
      super.cassandraClient.abort();
    }
  }
}
