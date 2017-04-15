package edu.berkeley.ground.dao.usage.postgres;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.berkeley.ground.dao.PostgresTest;
import edu.berkeley.ground.exceptions.GroundItemNotFoundException;
import edu.berkeley.ground.model.usage.LineageGraph;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.versions.VersionHistoryDag;
import edu.berkeley.ground.model.versions.VersionSuccessor;

import static org.junit.Assert.*;

public class PostgresLineageGraphFactoryTest extends PostgresTest {

  public PostgresLineageGraphFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageGraphCreation() throws GroundException {
    String testName = "test";
    String sourceKey = "testKey";

    PostgresTest.lineageGraphsResource.createLineageGraph(testName, sourceKey, new HashMap<>());
    LineageGraph graph = PostgresTest.lineageGraphsResource.getLineageGraph(sourceKey);

    assertEquals(testName, graph.getName());
    assertEquals(sourceKey, graph.getSourceKey());
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadLineageGraph() throws GroundException {
    String sourceKey = "test";

    try {
      PostgresTest.lineageGraphsResource.getLineageGraph(sourceKey);
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
      PostgresTest.lineageGraphsResource.createLineageGraph(lineageGraphName,
          lineageGraphKey, new HashMap<>());
    } catch (GroundException e) {
      fail(e.getMessage());
    }

    PostgresTest.lineageGraphsResource.createLineageGraph(lineageGraphName,
        lineageGraphKey, new HashMap<>());
  }

  @Test
  public void testTruncate() throws GroundException {
    String firstTestNode = "firstTestNode";
    long firstTestNodeId = PostgresTest.createNode(firstTestNode).getId();
    long firstNodeVersionId = PostgresTest.createNodeVersion(firstTestNodeId).getId();

    String secondTestNode = "secondTestNode";
    long secondTestNodeId = PostgresTest.createNode(secondTestNode).getId();
    long secondNodeVersionId = PostgresTest.createNodeVersion(secondTestNodeId).getId();

    String lineageEdgeName = "testLineageEdge";
    long lineageEdgeId = PostgresTest.createLineageEdge(lineageEdgeName).getId();
    long lineageEdgeVersionId = PostgresTest.createLineageEdgeVersion(lineageEdgeId,
        firstNodeVersionId, secondNodeVersionId).getId();

    List<Long> lineageEdgeVersionIds = new ArrayList<>();
    lineageEdgeVersionIds.add(lineageEdgeVersionId);

    String lineageGraphName = "testLineageGraph";
    long lineageGraphId = PostgresTest.createLineageGraph(lineageGraphName).getId();
    long lineageGraphVersionId = PostgresTest.createLineageGraphVersion(lineageGraphId,
        lineageEdgeVersionIds).getId();

    List<Long> parents = new ArrayList<>();
    parents.add(lineageGraphVersionId);
    long newLineageGraphVersionId = PostgresTest.createLineageGraphVersion(lineageGraphId,
        lineageEdgeVersionIds, parents).getId();

    PostgresTest.lineageGraphsResource.truncateLineageGraph(lineageGraphName, 1);

    VersionHistoryDag<?> dag = PostgresTest.versionHistoryDAGFactory
        .retrieveFromDatabase(lineageGraphId);

    assertEquals(1, dag.getEdgeIds().size());

    VersionSuccessor<?> successor = PostgresTest.versionSuccessorFactory.retrieveFromDatabase(
        dag.getEdgeIds().get(0));

    PostgresTest.postgresClient.commit();

    assertEquals(0, successor.getFromId());
    assertEquals(newLineageGraphVersionId, successor.getToId());
  }

}
