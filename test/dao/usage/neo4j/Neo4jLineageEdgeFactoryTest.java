package dao.usage.neo4j;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import dao.Neo4jTest;
import exceptions.GroundItemNotFoundException;
import models.usage.LineageEdge;
import exceptions.GroundException;
import models.versions.VersionHistoryDag;
import models.versions.VersionSuccessor;

import static org.junit.Assert.*;

public class Neo4jLineageEdgeFactoryTest extends Neo4jTest {

  public Neo4jLineageEdgeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageEdgeCreation() throws GroundException {
    try {
      String testName = "test";
      String sourceKey = "testKey";

      Neo4jTest.lineageEdgeFactory.create(testName, sourceKey, new HashMap<>());
      LineageEdge lineageEdge = Neo4jTest.lineageEdgeFactory.retrieveFromDatabase(sourceKey);

      assertEquals(testName, lineageEdge.getName());
      assertEquals(sourceKey, lineageEdge.getSourceKey());
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadLineageEdge() throws GroundException {
    String sourceKey = "test";

    try {
      Neo4jTest.lineageEdgeFactory.retrieveFromDatabase(sourceKey);
    } catch (GroundException e) {
      assertEquals(GroundItemNotFoundException.class, e.getClass());

      throw e;
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testCreateDuplicateLineageEdge() throws GroundException {
    String lineageEdgeName = "lineageEdgeName";
    String lineageEdgeKey = "lineageEdgeKey";

    try {
      try {
        Neo4jTest.lineageEdgeFactory.create(lineageEdgeName, lineageEdgeKey, new HashMap<>());
      } catch (GroundException e) {
        fail(e.getMessage());
      }

      Neo4jTest.lineageEdgeFactory.create(lineageEdgeName, lineageEdgeKey, new HashMap<>());
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }

  @Test
  public void testTruncate() throws GroundException {
    try {
      String firstTestNode = "firstTestNode";
      long firstTestNodeId = Neo4jTest.createNode(firstTestNode).getId();
      long firstNodeVersionId = Neo4jTest.createNodeVersion(firstTestNodeId).getId();

      String secondTestNode = "secondTestNode";
      long secondTestNodeId = Neo4jTest.createNode(secondTestNode).getId();
      long secondNodeVersionId = Neo4jTest.createNodeVersion(secondTestNodeId).getId();

      String lineageEdgeName = "testLineageEdge";
      long lineageEdgeId = Neo4jTest.createLineageEdge(lineageEdgeName).getId();

      long lineageEdgeVersionId = Neo4jTest.createLineageEdgeVersion(lineageEdgeId,
          firstNodeVersionId, secondNodeVersionId).getId();

      List<Long> parents = new ArrayList<>();
      parents.add(lineageEdgeVersionId);
      long newLineageEdgeVersionId = Neo4jTest.createLineageEdgeVersion(lineageEdgeId,
          firstNodeVersionId, secondNodeVersionId, parents).getId();

      Neo4jTest.lineageEdgeFactory.truncate(lineageEdgeId, 1);

      VersionHistoryDag<?> dag = Neo4jTest.versionHistoryDAGFactory
          .retrieveFromDatabase(lineageEdgeId);

      assertEquals(1, dag.getEdgeIds().size());

      VersionSuccessor<?> successor = Neo4jTest.versionSuccessorFactory.retrieveFromDatabase(
          dag.getEdgeIds().get(0));

      Neo4jTest.neo4jClient.commit();

      assertEquals(lineageEdgeId, successor.getFromId());
      assertEquals(newLineageEdgeVersionId, successor.getToId());
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }
}
