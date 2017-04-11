package edu.berkeley.ground.dao.usage.neo4j;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.berkeley.ground.dao.Neo4jTest;
import edu.berkeley.ground.exceptions.GroundItemNotFoundException;
import edu.berkeley.ground.model.usage.LineageGraph;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.versions.VersionHistoryDag;
import edu.berkeley.ground.model.versions.VersionSuccessor;

import static org.junit.Assert.*;

public class Neo4jLineageGraphFactoryTest extends Neo4jTest {

  public Neo4jLineageGraphFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageGraphCreation() throws GroundException {
    String testName = "test";
    String sourceKey = "testKey";

    Neo4jTest.lineageGraphsResource.createLineageGraph(testName, sourceKey, new HashMap<>());
    LineageGraph lineageGraph = Neo4jTest.lineageGraphsResource.getLineageGraph(sourceKey);

    assertEquals(testName, lineageGraph.getName());
    assertEquals(sourceKey, lineageGraph.getSourceKey());
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadLineageGraph() throws GroundException {
    String sourceKey = "test";

    try {
      Neo4jTest.lineageGraphsResource.getLineageGraph(sourceKey);
    } catch (GroundException e) {
      assertEquals(GroundItemNotFoundException.class, e.getClass());

      throw e;
    }
  }

  @Test(expected = GroundException.class)
  public void testCreateDuplicateLineageGraph() throws GroundException {
    String lineageGraphName = "lineageGraphName";
    String lineageGraphKey = "lineageGraphKey";

    try {
      Neo4jTest.lineageGraphsResource.createLineageGraph(lineageGraphName,
          lineageGraphKey, new HashMap<>());
    } catch (GroundException e) {
      fail(e.getMessage());
    }

    Neo4jTest.lineageGraphsResource.createLineageGraph(lineageGraphName,
        lineageGraphKey, new HashMap<>());
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

    List<Long> lineageEdgeVersionIds = new ArrayList<>();
    lineageEdgeVersionIds.add(lineageEdgeVersionId);

    String lineageGraphName = "testLineageGraph";
    long lineageGraphId = Neo4jTest.createLineageGraph(lineageGraphName).getId();

    long lineageGraphVersionId = Neo4jTest.createLineageGraphVersion(lineageGraphId,
        lineageEdgeVersionIds).getId();

    List<Long> parents = new ArrayList<>();
    parents.add(lineageGraphVersionId);
    long newLineageGraphVersionId = Neo4jTest.createLineageGraphVersion(lineageGraphId,
        lineageEdgeVersionIds, parents).getId();

    Neo4jTest.lineageGraphsResource.truncateLineageGraph(lineageGraphName, 1);

    VersionHistoryDag<?> dag = Neo4jTest.versionHistoryDAGFactory
        .retrieveFromDatabase(lineageGraphId);

    assertEquals(1, dag.getEdgeIds().size());

    VersionSuccessor<?> successor = Neo4jTest.versionSuccessorFactory.retrieveFromDatabase(
        dag.getEdgeIds().get(0));

    Neo4jTest.neo4jClient.commit();

    assertEquals(lineageGraphId, successor.getFromId());
    assertEquals(newLineageGraphVersionId, successor.getToId());
  }
}
