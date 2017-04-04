package edu.berkeley.ground.dao.versions.neo4j;

import edu.berkeley.ground.dao.Neo4jTest;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.versions.VersionHistoryDag;
import edu.berkeley.ground.model.versions.VersionSuccessor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import static org.junit.Assert.*;

public class Neo4jItemFactoryTest extends Neo4jTest {
  public Neo4jItemFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testCorrectUpdateWithParent() throws GroundException {
    try {
      long testNodeId = super.factories.getNodeFactory().create("testNode", null, new HashMap<>())
          .getId();

      long fromNodeId = super.factories.getNodeFactory().create("testFromNode", null,
          new HashMap<>()).getId();
      long toNodeId = super.factories.getNodeFactory().create("testToNode", null, new HashMap<>())
          .getId();
      long fromId = super.createNodeVersion(fromNodeId);
      long toId = super.createNodeVersion(toNodeId);

      List<Long> parentIds = new ArrayList<>();
      super.itemFactory.update(testNodeId, fromId, parentIds);

      parentIds.clear();
      parentIds.add(fromId);
      super.itemFactory.update(testNodeId, toId, parentIds);

      VersionHistoryDag<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(testNodeId);

      assertEquals(2, dag.getEdgeIds().size());
      assertEquals(toId, (long) dag.getLeaves().get(0));

      VersionSuccessor<?> successor = null;
      for (long id : dag.getEdgeIds()) {
        successor = super.versionSuccessorFactory.retrieveFromDatabase(id);

        if (successor.getFromId() != testNodeId) {
          break;
        }
      }

      if (successor == null) {
        fail();
      }

      assertEquals(fromId, successor.getFromId());
      assertEquals(toId, successor.getToId());

      super.neo4jClient.commit();
    } finally {
      super.neo4jClient.abort();
    }
  }

  @Test
  public void testCorrectUpdateWithoutParent() throws GroundException {
    try {
      long fromNodeId = super.factories.getNodeFactory().create("testFromNode", null,
          new HashMap<>()).getId();
      long toNodeId = super.factories.getNodeFactory().create("testToNode", null, new HashMap<>())
          .getId();
      long toId = super.createNodeVersion(toNodeId);

      List<Long> parentIds = new ArrayList<>();

      // No parent is specified, and there is no other version in this Item, we should
      // automatically make this a child of EMPTY
      super.itemFactory.update(fromNodeId, toId, parentIds);

      VersionHistoryDag<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(fromNodeId);

      assertEquals(1, dag.getEdgeIds().size());
      assertEquals(toId, (long) dag.getLeaves().get(0));

      VersionSuccessor<?> successor = super.versionSuccessorFactory.retrieveFromDatabase(
          dag.getEdgeIds().get(0));

      assertEquals(fromNodeId, successor.getFromId());
      assertEquals(toId, successor.getToId());

      super.neo4jClient.commit();
    } finally {
      super.neo4jClient.abort();
    }
  }

  @Test
  public void testCorrectUpdateWithLinearHistory() throws GroundException {
    try {
      long testNodeId = super.factories.getNodeFactory().create("testNode", null, new HashMap<>())
          .getId();

      long fromNodeId = super.factories.getNodeFactory().create("testFromNode", null,
          new HashMap<>()).getId();
      long toNodeId = super.factories.getNodeFactory().create("testToNode", null, new HashMap<>())
          .getId();
      long fromId = super.createNodeVersion(fromNodeId);
      long toId = super.createNodeVersion(toNodeId);

      List<Long> parentIds = new ArrayList<>();

      // first, make from a child of EMPTY
      super.itemFactory.update(testNodeId, fromId, parentIds);

      // then, add to as a child and make sure that it becomes a child of from
      parentIds.clear();
      parentIds.add(fromId);
      super.itemFactory.update(testNodeId, toId, parentIds);

      VersionHistoryDag<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(testNodeId);

      assertEquals(2, dag.getEdgeIds().size());
      assertEquals(toId, (long) dag.getLeaves().get(0));

      VersionSuccessor<?> fromSuccessor = super.versionSuccessorFactory.retrieveFromDatabase(
          dag.getEdgeIds().get(0));

      VersionSuccessor<?> toSuccessor = super.versionSuccessorFactory.retrieveFromDatabase(
          dag.getEdgeIds().get(1));

      if (fromSuccessor.getFromId() != testNodeId) {
        VersionSuccessor<?> tmp = fromSuccessor;
        fromSuccessor = toSuccessor;
        toSuccessor = tmp;
      }

      assertEquals(testNodeId, fromSuccessor.getFromId());
      assertEquals(fromId, fromSuccessor.getToId());

      assertEquals(fromId, toSuccessor.getFromId());
      assertEquals(toId, toSuccessor.getToId());

      super.neo4jClient.commit();
    } finally {
      super.neo4jClient.abort();
    }
  }

  @Test(expected = GroundException.class)
  public void testIncorrectUpdate() throws GroundException {
    try {
      long fromNodeId = super.factories.getNodeFactory().create("testFromNode", null,
          new HashMap<>()).getId();
      long toNodeId = super.factories.getNodeFactory().create("testToNode", null, new HashMap<>())
          .getId();
      long fromId = 1123;
      long toId = super.createNodeVersion(toNodeId);

      List<Long> parentIds = new ArrayList<>();
      parentIds.add(fromId);

      // this should fail because fromId is not a valid version
      super.itemFactory.update(fromNodeId, toId, parentIds);

      super.neo4jClient.commit();
    } finally {
      super.neo4jClient.abort();
    }
  }

  @Test
  public void testMultipleParents() throws GroundException {
    try {
      long testNodeId = super.factories.getNodeFactory().create("testNode", null, new HashMap<>())
          .getId();

      long parentOneNodeId = super.factories.getNodeFactory().create("testFromNode", null,
          new HashMap<>()).getId();
      long parentTwoNodeId = super.factories.getNodeFactory().create("testToNode", null,
          new HashMap<>()).getId();
      long parentOne = super.createNodeVersion(parentOneNodeId);
      long parentTwo = super.createNodeVersion(parentTwoNodeId);

      long childNodeId = super.factories.getNodeFactory().create("testChildNode", null,
          new HashMap<>()).getId();
      long child = super.createNodeVersion(childNodeId);


      List<Long> parentIds = new ArrayList<>();

      // first, make the parents children of EMPTY
      super.itemFactory.update(testNodeId, parentOne, parentIds);
      super.itemFactory.update(testNodeId, parentTwo, parentIds);

      // then, add to as a child and make sure that it becomes a child of from
      parentIds.clear();
      parentIds.add(parentOne);
      parentIds.add(parentTwo);
      super.itemFactory.update(testNodeId, child, parentIds);

      VersionHistoryDag<?> dag = super.versionHistoryDAGFactory
          .retrieveFromDatabase(testNodeId);

      assertEquals(4, dag.getEdgeIds().size());
      assertEquals(child, (long) dag.getLeaves().get(0));

      // Retrieve all the version successors and check that they have the correct data.
      Set<List<Long>> correctSuccessors = new HashSet<>();
      correctSuccessors.add(Arrays.asList(testNodeId, parentOne));
      correctSuccessors.add(Arrays.asList(testNodeId, parentTwo));
      correctSuccessors.add(Arrays.asList(parentOne, child));
      correctSuccessors.add(Arrays.asList(parentTwo, child));

      for (long edgeId : dag.getEdgeIds()) {
        VersionSuccessor<?> successor = super.versionSuccessorFactory.retrieveFromDatabase(edgeId);

        correctSuccessors.remove(Arrays.asList(successor.getFromId(), successor.getToId()));
      }

      assertTrue(correctSuccessors.isEmpty());

      super.neo4jClient.commit();
    } finally {
      super.neo4jClient.abort();
    }
  }
}
