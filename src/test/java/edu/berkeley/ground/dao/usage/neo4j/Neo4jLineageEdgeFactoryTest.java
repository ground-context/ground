package edu.berkeley.ground.dao.usage.neo4j;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.berkeley.ground.dao.Neo4jTest;
import edu.berkeley.ground.exceptions.GroundItemNotFoundException;
import edu.berkeley.ground.model.usage.LineageEdge;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.versions.VersionHistoryDag;
import edu.berkeley.ground.model.versions.VersionSuccessor;

import static org.junit.Assert.*;

public class Neo4jLineageEdgeFactoryTest extends Neo4jTest {

  public Neo4jLineageEdgeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageEdgeCreation() throws GroundException {
    String testName = "test";
    String sourceKey = "testKey";

    Neo4jTest.lineageEdgesResource.createLineageEdge(testName, sourceKey, new HashMap<>());
    LineageEdge lineageEdge = Neo4jTest.lineageEdgesResource.getLineageEdge(sourceKey);

    assertEquals(testName, lineageEdge.getName());
    assertEquals(sourceKey, lineageEdge.getSourceKey());
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadLineageEdge() throws GroundException {
    String sourceKey = "test";

    try {
      Neo4jTest.lineageEdgesResource.getLineageEdge(sourceKey);
    } catch (GroundException e) {
      assertEquals(GroundItemNotFoundException.class, e.getClass());

      throw e;
    }
  }

  @Test(expected = GroundException.class)
  public void testCreateDuplicateLineageEdge() throws GroundException {
    String lineageEdgeName = "lineageEdgeName";
    String lineageEdgeKey = "lineageEdgeKey";

    try {
      Neo4jTest.lineageEdgesResource.createLineageEdge(lineageEdgeName, lineageEdgeKey, new HashMap<>());
    } catch (GroundException e) {
      fail(e.getMessage());
    }

    Neo4jTest.lineageEdgesResource.createLineageEdge(lineageEdgeName, lineageEdgeKey, new HashMap<>());
  }

  @Test
  public void testTruncate() throws GroundException {
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

    Neo4jTest.lineageEdgesResource.truncateLineageEdge(lineageEdgeName, 1);

    VersionHistoryDag<?> dag = Neo4jTest.versionHistoryDAGFactory
        .retrieveFromDatabase(lineageEdgeId);

    assertEquals(1, dag.getEdgeIds().size());

    VersionSuccessor<?> successor = Neo4jTest.versionSuccessorFactory.retrieveFromDatabase(
        dag.getEdgeIds().get(0));

    Neo4jTest.neo4jClient.commit();

    assertEquals(lineageEdgeId, successor.getFromId());
    assertEquals(newLineageEdgeVersionId, successor.getToId());
  }
}
