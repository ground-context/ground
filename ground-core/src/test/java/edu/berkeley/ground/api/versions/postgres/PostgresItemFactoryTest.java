package edu.berkeley.ground.api.versions.postgres;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import edu.berkeley.ground.api.PostgresTest;
import edu.berkeley.ground.api.versions.VersionHistoryDAG;
import edu.berkeley.ground.api.versions.VersionSuccessor;
import edu.berkeley.ground.db.PostgresClient.PostgresConnection;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class PostgresItemFactoryTest extends PostgresTest {

  public PostgresItemFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testCorrectUpdateWithParent() throws GroundException {
    PostgresConnection connection = null;

    try {
      long testId = 1;
      connection = super.cassandraClient.getConnection();

      super.itemFactory.insertIntoDatabase(connection, testId);

      long fromId = 2;
      long toId = 3;

      super.versionFactory.insertIntoDatabase(connection, fromId);
      super.versionFactory.insertIntoDatabase(connection, toId);

      List<Long> parentIds = new ArrayList<>();
      super.itemFactory.update(connection, testId, fromId, parentIds);

      parentIds.clear();
      parentIds.add(fromId);
      super.itemFactory.update(connection, testId, toId, parentIds);

      VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(connection,
          testId);

      assertEquals(2, dag.getEdgeIds().size());
      assertEquals(toId, (long) dag.getLeaves().get(0));

      VersionSuccessor<?> successor = null;
      for (long id : dag.getEdgeIds()) {
        successor = super.versionSuccessorFactory.retrieveFromDatabase(connection, id);

        if (!(successor.getFromId() == 0)) {
          break;
        }
      }

      if (successor != null) {
        assertEquals(fromId, successor.getFromId());
        assertEquals(toId, successor.getToId());

        return;
      }

      fail();
    } finally {
      connection.abort();
    }
  }

  @Test
  public void testCorrectUpdateWithoutParent() throws GroundException {
    PostgresConnection connection = null;

    try {
      long testId = 1;
      connection = super.cassandraClient.getConnection();

      super.itemFactory.insertIntoDatabase(connection, testId);
      long toId = 2;
      super.versionFactory.insertIntoDatabase(connection, toId);

      List<Long> parentIds = new ArrayList<>();

      // No parent is specified, and there is no other version in this Item, we should
      // automatically make this a child of EMPTY
      super.itemFactory.update(connection, testId, toId, parentIds);

      VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(connection,
          testId);

      assertEquals(1, dag.getEdgeIds().size());
      assertEquals(toId, (long) dag.getLeaves().get(0));

      VersionSuccessor<?> successor = super.versionSuccessorFactory.retrieveFromDatabase(
          connection, dag.getEdgeIds().get(0));

      assertEquals(0, successor.getFromId());
      assertEquals(toId, successor.getToId());
    } finally {
      connection.abort();
    }
  }

  @Test
  public void testCorrectUpdateWithLinearHistory() throws GroundException {
    PostgresConnection connection = null;
    try {
      long testId = 1;
      connection = super.cassandraClient.getConnection();

      super.itemFactory.insertIntoDatabase(connection, testId);

      long fromId = 2;
      long toId = 3;

      super.versionFactory.insertIntoDatabase(connection, fromId);
      super.versionFactory.insertIntoDatabase(connection, toId);
      List<Long> parentIds = new ArrayList<>();

      // first, make from a child of EMPTY
      super.itemFactory.update(connection, testId, fromId, parentIds);

      // then, add to as a child and make sure that it becomes a child of from
      parentIds.clear();
      parentIds.add(fromId);
      super.itemFactory.update(connection, testId, toId, parentIds);

      VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(connection,
          testId);

      assertEquals(2, dag.getEdgeIds().size());
      assertEquals(toId, (long) dag.getLeaves().get(0));

      VersionSuccessor<?> fromSuccessor = super.versionSuccessorFactory.retrieveFromDatabase(
          connection, dag.getEdgeIds().get(0));

      VersionSuccessor<?> toSuccessor = super.versionSuccessorFactory.retrieveFromDatabase(
          connection, dag.getEdgeIds().get(1));

      if (!(fromSuccessor.getFromId() == 0)) {
        VersionSuccessor<?> tmp = fromSuccessor;
        fromSuccessor = toSuccessor;
        toSuccessor = tmp;
      }

      assertEquals(0, fromSuccessor.getFromId());
      assertEquals(fromId, fromSuccessor.getToId());

      assertEquals(fromId, toSuccessor.getFromId());
      assertEquals(toId, toSuccessor.getToId());
    } finally {
      connection.abort();
    }
  }

  @Test(expected = GroundException.class)
  public void testIncorrectUpdate() throws GroundException {
    PostgresConnection connection = null;

    try {
      long testId = 1;
      long fromId = 2;
      long toId = 3;

      try {
        connection = super.cassandraClient.getConnection();

        super.itemFactory.insertIntoDatabase(connection, testId);

        super.versionFactory.insertIntoDatabase(connection, toId);

      } catch (GroundException ge) {
        fail(ge.getMessage());
      }

      List<Long> parentIds = new ArrayList<>();
      parentIds.add(fromId);

      // this should fail because fromId is not a valid version
      super.itemFactory.update(connection, testId, toId, parentIds);
    } finally {
      connection.abort();
    }
  }

  @Test
  public void testMultipleParents() throws GroundException {
    PostgresConnection connection = null;
    try {
      long testId = 1;
      connection = super.cassandraClient.getConnection();

      super.itemFactory.insertIntoDatabase(connection, testId);

      long parentOne = 2;
      long parentTwo = 3;
      long child = 4;

      super.versionFactory.insertIntoDatabase(connection, parentOne);
      super.versionFactory.insertIntoDatabase(connection, parentTwo);
      super.versionFactory.insertIntoDatabase(connection, child);
      List<Long> parentIds = new ArrayList<>();

      // first, make the parents children of EMPTY
      super.itemFactory.update(connection, testId, parentOne, parentIds);
      super.itemFactory.update(connection, testId, parentTwo, parentIds);

      // then, add to as a child and make sure that it becomes a child of from
      parentIds.clear();
      parentIds.add(parentOne);
      parentIds.add(parentTwo);
      super.itemFactory.update(connection, testId, child, parentIds);

      VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(connection,
          testId);

      assertEquals(4, dag.getEdgeIds().size());
      assertEquals(child, (long) dag.getLeaves().get(0));

      // Retrieve all the version successors and check that they have the correct data.
      VersionSuccessor<?> parentOneSuccessor = super.versionSuccessorFactory.retrieveFromDatabase(
          connection, dag.getEdgeIds().get(0));

      VersionSuccessor<?> parentTwoSuccessor = super.versionSuccessorFactory.retrieveFromDatabase(
          connection, dag.getEdgeIds().get(1));

      VersionSuccessor<?> childOneSuccessor = super.versionSuccessorFactory.retrieveFromDatabase(
          connection, dag.getEdgeIds().get(2));

      VersionSuccessor<?> childTwoSuccessor = super.versionSuccessorFactory.retrieveFromDatabase(
          connection, dag.getEdgeIds().get(3));

      assertEquals(0, parentOneSuccessor.getFromId());
      assertEquals(parentOne, parentOneSuccessor.getToId());

      assertEquals(0, parentTwoSuccessor.getFromId());
      assertEquals(parentTwo, parentTwoSuccessor.getToId());

      assertEquals(parentOne, childOneSuccessor.getFromId());
      assertEquals(child, childOneSuccessor.getToId());

      assertEquals(parentTwo, childTwoSuccessor.getFromId());
      assertEquals(child, childTwoSuccessor.getToId());
      assertEquals(child, childTwoSuccessor.getToId());
    } finally {
      connection.abort();
    }
  }
}
