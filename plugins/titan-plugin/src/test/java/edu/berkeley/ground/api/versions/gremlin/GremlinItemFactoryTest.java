package edu.berkeley.ground.api.versions.gremlin;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import edu.berkeley.ground.api.GremlinTest;
import edu.berkeley.ground.api.versions.VersionHistoryDAG;
import edu.berkeley.ground.api.versions.VersionSuccessor;
import edu.berkeley.ground.db.GremlinClient.GremlinConnection;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class GremlinItemFactoryTest extends GremlinTest {
  public GremlinItemFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testCorrectUpdateWithParent() throws GroundException {
    GremlinConnection connection = null;
    try {
      String testId = super.factories.getNodeFactory().create("test").getId();
      connection = super.gremlinClient.getConnection();

      super.itemFactory.insertIntoDatabase(connection, testId);

      String fromNodeId = super.factories.getNodeFactory().create("testFromId").getId();
      String toNodeId = super.factories.getNodeFactory().create("testToId").getId();

      String fromId = super.createNodeVersion(fromNodeId);
      String toId = super.createNodeVersion(toNodeId);

      List<String> parentIds = new ArrayList<>();
      parentIds.add(fromId);

      super.itemFactory.update(connection, testId, toId, parentIds);

      VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(connection,
          testId);

      assertEquals(1, dag.getEdgeIds().size());
      assertEquals(toId, dag.getLeaves().get(0));

      VersionSuccessor<?> successor = super.versionSuccessorFactory.retrieveFromDatabase(
          connection, dag.getEdgeIds().get(0));

      assertEquals(fromId, successor.getFromId());
      assertEquals(toId, successor.getToId());
    } finally {
      connection.abort();
    }
  }

  @Test
  public void testCorrectUpdateWithoutParent() throws GroundException {
    GremlinConnection connection = null;
    try {
      String testId = super.factories.getNodeFactory().create("test").getId();
      connection = super.gremlinClient.getConnection();

      super.itemFactory.insertIntoDatabase(connection, testId);

      String toNodeId = super.factories.getNodeFactory().create("testToId").getId();

      String toId = super.createNodeVersion(toNodeId);

      List<String> parentIds = new ArrayList<>();

      // No parent is specified, and there is no other version in this Item, we should
      // automatically make this a child of EMPTY
      super.itemFactory.update(connection, testId, toId, parentIds);

      VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(connection,
          testId);

      assertEquals(1, dag.getEdgeIds().size());
      assertEquals(toId, dag.getLeaves().get(0));

      VersionSuccessor<?> successor = super.versionSuccessorFactory.retrieveFromDatabase(
          connection, dag.getEdgeIds().get(0));

      assertEquals(testId, successor.getFromId());
      assertEquals(toId, successor.getToId());
    } finally {
      connection.abort();
    }
  }

  @Test
  public void testCorrectUpdateWithLinearHistory() throws GroundException {
    GremlinConnection connection = null;
    try {
      String testId = super.factories.getNodeFactory().create("test").getId();
      String fromNodeId = super.factories.getNodeFactory().create("testFromId").getId();
      String toNodeId = super.factories.getNodeFactory().create("testToId").getId();

      String fromId = super.createNodeVersion(fromNodeId);
      String toId = super.createNodeVersion(toNodeId);

      connection = super.gremlinClient.getConnection();
      List<String> parentIds = new ArrayList<>();

      // first, make from a child of EMPTY
      super.itemFactory.update(connection, testId, fromId, parentIds);

      // then, add to as a child and make sure that it becomes a child of from
      parentIds.clear();
      parentIds.add(fromId);
      super.itemFactory.update(connection, testId, toId, parentIds);

      VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(connection,
          testId);

      assertEquals(2, dag.getEdgeIds().size());
      assertEquals(toId, dag.getLeaves().get(0));

      VersionSuccessor<?> fromSuccessor = super.versionSuccessorFactory.retrieveFromDatabase(
          connection, dag.getEdgeIds().get(0));

      VersionSuccessor<?> toSuccessor = super.versionSuccessorFactory.retrieveFromDatabase(
          connection, dag.getEdgeIds().get(1));

      assertEquals(testId, fromSuccessor.getFromId());
      assertEquals(fromId, fromSuccessor.getToId());

      assertEquals(fromId, toSuccessor.getFromId());
      assertEquals(toId, toSuccessor.getToId());
    } finally {
      connection.abort();
    }
  }

  @Test(expected = GroundException.class)
  public void testIncorrectUpdate() throws GroundException {
    GremlinConnection connection = null;

    try {
      String testId = super.factories.getNodeFactory().create("testNodeId").getId();
      String fromId = "someRandomId";
      String toId = null;

      try {
        connection = super.gremlinClient.getConnection();
        super.itemFactory.insertIntoDatabase(connection, testId);

        toId = super.createNodeVersion(testId);
      } catch (GroundException ge) {
        fail(ge.getMessage());
      }

      List<String> parentIds = new ArrayList<>();
      parentIds.add(fromId);

      // this should fail because fromId is not a valid version
      super.itemFactory.update(connection, testId, toId, parentIds);
    } finally {
      connection.abort();
    }
  }

  @Test
  public void testMultipleParents() throws GroundException {
    GremlinConnection connection = null;

    try {
      String testId = super.factories.getNodeFactory().create("test").getId();
      connection = super.gremlinClient.getConnection();

      super.itemFactory.insertIntoDatabase(connection, testId);

      String parentOneNodeId = super.factories.getNodeFactory().create("parentOneNode").getId();
      String parentTwoNodeId = super.factories.getNodeFactory().create("parentOneNode").getId();
      String childNodeId = super.factories.getNodeFactory().create("parentOneNode").getId();
      String parentOne = super.createNodeVersion(parentOneNodeId);
      String parentTwo = super.createNodeVersion(parentTwoNodeId);
      String child = super.createNodeVersion(childNodeId);

      List<String> parentIds = new ArrayList<>();

      // first, make the parents children of EMPTY
      super.itemFactory.update(connection, testId, parentOne, parentIds);
      super.itemFactory.update(connection, testId, parentTwo, parentIds);

      parentIds.clear();
      parentIds.add(parentOne);
      parentIds.add(parentTwo);
      // then, add to as a child and make sure that it becomes a child of from
      super.itemFactory.update(connection, testId, child, parentIds);

      VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(connection,
          testId);

      assertEquals(4, dag.getEdgeIds().size());
      assertEquals(child, dag.getLeaves().get(0));

      // Retrieve all the version successors and check that they have the correct data.
      VersionSuccessor<?> parentOneSuccessor = super.versionSuccessorFactory.retrieveFromDatabase(
          connection, dag.getEdgeIds().get(0));

      VersionSuccessor<?> parentTwoSuccessor = super.versionSuccessorFactory.retrieveFromDatabase(
          connection, dag.getEdgeIds().get(1));

      VersionSuccessor<?> childOneSuccessor = super.versionSuccessorFactory.retrieveFromDatabase(
          connection, dag.getEdgeIds().get(2));

      VersionSuccessor<?> childTwoSuccessor = super.versionSuccessorFactory.retrieveFromDatabase(
          connection, dag.getEdgeIds().get(3));

      assertEquals(testId, parentOneSuccessor.getFromId());
      assertEquals(parentOne, parentOneSuccessor.getToId());

      assertEquals(testId, parentTwoSuccessor.getFromId());
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
