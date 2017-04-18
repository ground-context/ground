package edu.berkeley.ground.dao.versions.neo4j;

import edu.berkeley.ground.dao.Neo4jTest;
import edu.berkeley.ground.dao.models.neo4j.Neo4jTagFactory;
import edu.berkeley.ground.dao.versions.neo4j.mock.TestNeo4jItemFactory;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.versions.Item;
import edu.berkeley.ground.model.versions.Version;
import edu.berkeley.ground.model.versions.VersionHistoryDag;
import edu.berkeley.ground.model.versions.VersionSuccessor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import static org.junit.Assert.*;

public class Neo4jItemFactoryTest extends Neo4jTest {

  private TestNeo4jItemFactory itemFactory;

  public Neo4jItemFactoryTest() throws GroundException {
    super();

    this.itemFactory = new TestNeo4jItemFactory(Neo4jTest.neo4jClient,
        Neo4jTest.versionHistoryDAGFactory, Neo4jTest.tagFactory);
  }

  @Test
  public void testCorrectUpdateWithParent() throws GroundException {
    try {
      long testNodeId = Neo4jTest.createNode("testNode").getId();

      long fromNodeId = Neo4jTest.createNode("testFromNode").getId();
      long toNodeId = Neo4jTest.createNode("testToNode").getId();
      long fromId = Neo4jTest.createNodeVersion(fromNodeId).getId();
      long toId = Neo4jTest.createNodeVersion(toNodeId).getId();

      List<Long> parentIds = new ArrayList<>();
      this.itemFactory.update(testNodeId, fromId, parentIds);

      parentIds.clear();
      parentIds.add(fromId);
      this.itemFactory.update(testNodeId, toId, parentIds);

      VersionHistoryDag<?> dag = Neo4jTest.versionHistoryDAGFactory.retrieveFromDatabase(testNodeId);

      assertEquals(2, dag.getEdgeIds().size());
      assertEquals(toId, (long) dag.getLeaves().get(0));

      VersionSuccessor<?> successor = null;
      for (long id : dag.getEdgeIds()) {
        successor = Neo4jTest.versionSuccessorFactory.retrieveFromDatabase(id);

        if (successor.getFromId() != testNodeId) {
          break;
        }
      }

      if (successor == null) {
        fail();
      }

      assertEquals(fromId, successor.getFromId());
      assertEquals(toId, successor.getToId());

      Neo4jTest.neo4jClient.commit();
    } finally {
      Neo4jTest.neo4jClient.abort();
    }
  }

  @Test
  public void testCorrectUpdateWithoutParent() throws GroundException {
    try {
      long testFromNode = Neo4jTest.createNode("testFrom").getId();
      long testToNode = Neo4jTest.createNode("testTo").getId();
      long toId = Neo4jTest.createNodeVersion(testToNode).getId();

      List<Long> parentIds = new ArrayList<>();

      // No parent is specified, and there is no other version in this Item, we should
      // automatically make this a child of EMPTY
      this.itemFactory.update(testFromNode, toId, parentIds);
      Neo4jTest.neo4jClient.commit();

      VersionHistoryDag<?> dag = Neo4jTest.versionHistoryDAGFactory
          .retrieveFromDatabase(testFromNode);

      assertEquals(1, dag.getEdgeIds().size());
      assertEquals(toId, (long) dag.getLeaves().get(0));

      VersionSuccessor<?> successor = Neo4jTest.versionSuccessorFactory.retrieveFromDatabase(
          dag.getEdgeIds().get(0));

      assertEquals(testFromNode, successor.getFromId());
      assertEquals(toId, successor.getToId());

      Neo4jTest.neo4jClient.commit();
    } finally {
      Neo4jTest.neo4jClient.abort();
    }
  }

  @Test
  public void testCorrectUpdateWithLinearHistory() throws GroundException {
    try {
      long testNodeId = Neo4jTest.createNode("testNode").getId();

      long fromNodeId = Neo4jTest.createNode("testFromNode").getId();
      long toNodeId = Neo4jTest.createNode("testToNode").getId();

      long fromId = Neo4jTest.createNodeVersion(fromNodeId).getId();
      long toId = Neo4jTest.createNodeVersion(toNodeId).getId();

      List<Long> parentIds = new ArrayList<>();

      // first, make from a child of EMPTY
      this.itemFactory.update(testNodeId, fromId, parentIds);

      // then, add to as a child and make sure that it becomes a child of from
      parentIds.clear();
      parentIds.add(fromId);
      this.itemFactory.update(testNodeId, toId, parentIds);

      VersionHistoryDag<?> dag = Neo4jTest.versionHistoryDAGFactory.retrieveFromDatabase(testNodeId);

      assertEquals(2, dag.getEdgeIds().size());
      assertEquals(toId, (long) dag.getLeaves().get(0));

      VersionSuccessor<?> fromSuccessor = Neo4jTest.versionSuccessorFactory.retrieveFromDatabase(
          dag.getEdgeIds().get(0));

      VersionSuccessor<?> toSuccessor = Neo4jTest.versionSuccessorFactory.retrieveFromDatabase(
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

      Neo4jTest.neo4jClient.commit();
    } finally {
      Neo4jTest.neo4jClient.abort();
    }
  }

  @Test(expected = GroundException.class)
  public void testIncorrectUpdate() throws GroundException {
    try {
      long fromNodeId = Neo4jTest.createNode("testFromNode").getId();
      long toNodeId = Neo4jTest.createNode("testToNode").getId();

      long fromId = 1123;
      long toId = Neo4jTest.createNodeVersion(toNodeId).getId();

      List<Long> parentIds = new ArrayList<>();
      parentIds.add(fromId);

      // this should fail because fromId is not a valid version
      this.itemFactory.update(fromNodeId, toId, parentIds);

      Neo4jTest.neo4jClient.commit();
    } finally {
      Neo4jTest.neo4jClient.abort();
    }
  }

  @Test
  public void testMultipleParents() throws GroundException {
    try {
      long testNodeId = Neo4jTest.createNode("testNode").getId();

      long parentOneNodeId = Neo4jTest.createNode("testFromNode").getId();
      long parentTwoNodeId = Neo4jTest.createNode("testToNode").getId();

      long parentOne = Neo4jTest.createNodeVersion(parentOneNodeId).getId();
      long parentTwo = Neo4jTest.createNodeVersion(parentTwoNodeId).getId();

      long childNodeId = Neo4jTest.createNode("testChildNode").getId();
      long child = Neo4jTest.createNodeVersion(childNodeId).getId();


      List<Long> parentIds = new ArrayList<>();

      // first, make the parents children of EMPTY
      this.itemFactory.update(testNodeId, parentOne, parentIds);
      this.itemFactory.update(testNodeId, parentTwo, parentIds);

      // then, add to as a child and make sure that it becomes a child of from
      parentIds.clear();
      parentIds.add(parentOne);
      parentIds.add(parentTwo);
      this.itemFactory.update(testNodeId, child, parentIds);

      VersionHistoryDag<?> dag = Neo4jTest.versionHistoryDAGFactory
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
        VersionSuccessor<?> successor = Neo4jTest.versionSuccessorFactory.retrieveFromDatabase(edgeId);

        correctSuccessors.remove(Arrays.asList(successor.getFromId(), successor.getToId()));
      }

      assertTrue(correctSuccessors.isEmpty());

      Neo4jTest.neo4jClient.commit();
    } finally {
      Neo4jTest.neo4jClient.abort();
    }
  }
}
