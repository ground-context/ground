package dao.usage.neo4j;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import dao.Neo4jTest;
import exceptions.GroundItemNotFoundException;
import models.usage.LineageGraph;
import exceptions.GroundException;
import models.versions.VersionHistoryDag;
import models.versions.VersionSuccessor;

import static org.junit.Assert.*;

public class Neo4jLineageGraphFactoryTest extends Neo4jTest {

  public Neo4jLineageGraphFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageGraphCreation() throws GroundException {
    try {
      String testName = "test";
      String sourceKey = "testKey";

      Neo4jTest.lineageGraphFactory.create(testName, sourceKey, new HashMap<>());
      LineageGraph lineageGraph = Neo4jTest.lineageGraphFactory.retrieveFromDatabase(sourceKey);

      assertEquals(testName, lineageGraph.getName());
      assertEquals(sourceKey, lineageGraph.getSourceKey());
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadLineageGraph() throws GroundException {
    String sourceKey = "test";

    try {
      Neo4jTest.lineageGraphFactory.retrieveFromDatabase(sourceKey);
    } catch (GroundException e) {
      assertEquals(GroundItemNotFoundException.class, e.getClass());

      throw e;
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testCreateDuplicateLineageGraph() throws GroundException {
    String lineageGraphName = "lineageGraphName";
    String lineageGraphKey = "lineageGraphKey";

    try {
      try {
        Neo4jTest.lineageGraphFactory.create(lineageGraphName, lineageGraphKey, new HashMap<>());
      } catch (GroundException e) {
        fail(e.getMessage());
      }

      Neo4jTest.lineageGraphFactory.create(lineageGraphName, lineageGraphKey, new HashMap<>());
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

      Neo4jTest.lineageGraphFactory.truncate(lineageGraphId, 1);

      VersionHistoryDag<?> dag = Neo4jTest.versionHistoryDAGFactory
          .retrieveFromDatabase(lineageGraphId);

      assertEquals(1, dag.getEdgeIds().size());

      VersionSuccessor<?> successor = Neo4jTest.versionSuccessorFactory.retrieveFromDatabase(
          dag.getEdgeIds().get(0));

      Neo4jTest.neo4jClient.commit();

      assertEquals(lineageGraphId, successor.getFromId());
      assertEquals(newLineageGraphVersionId, successor.getToId());
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }
}
