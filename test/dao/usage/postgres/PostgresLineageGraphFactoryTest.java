package dao.usage.postgres;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import dao.PostgresTest;
import exceptions.GroundItemNotFoundException;
import models.usage.LineageGraph;
import exceptions.GroundException;
import models.versions.VersionHistoryDag;
import models.versions.VersionSuccessor;

import static org.junit.Assert.*;

public class PostgresLineageGraphFactoryTest extends PostgresTest {

  public PostgresLineageGraphFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageGraphCreation() throws GroundException {
    try {
      String testName = "test";
      String sourceKey = "testKey";

      PostgresTest.lineageGraphFactory.create(testName, sourceKey, new HashMap<>());
      LineageGraph graph = PostgresTest.lineageGraphFactory.retrieveFromDatabase(sourceKey);

      assertEquals(testName, graph.getName());
      assertEquals(sourceKey, graph.getSourceKey());
    } finally {
      PostgresTest.postgresClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadLineageGraph() throws GroundException {
    String sourceKey = "test";

    try {
      PostgresTest.lineageGraphFactory.retrieveFromDatabase(sourceKey);
    } catch (GroundException e) {
      assertEquals(GroundItemNotFoundException.class, e.getClass());

      throw e;
    } finally {
      PostgresTest.postgresClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testCreateDuplicateLineageGraph() throws GroundException {
    String lineageGraphName = "lineageGraphName";
    String lineageGraphKey = "lineageGraphKey";

    try {
      try {
        PostgresTest.lineageGraphFactory.create(lineageGraphName, lineageGraphKey, new HashMap<>());
      } catch (GroundException e) {
        fail(e.getMessage());
      }

      PostgresTest.lineageGraphFactory.create(lineageGraphName, lineageGraphKey, new HashMap<>());
    } finally {
      PostgresTest.postgresClient.commit();
    }
  }

  @Test
  public void testTruncate() throws GroundException {
    try {
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

      PostgresTest.lineageGraphFactory.truncate(lineageGraphId, 1);

      VersionHistoryDag<?> dag = PostgresTest.versionHistoryDAGFactory
          .retrieveFromDatabase(lineageGraphId);

      assertEquals(1, dag.getEdgeIds().size());

      VersionSuccessor<?> successor = PostgresTest.versionSuccessorFactory.retrieveFromDatabase(
          dag.getEdgeIds().get(0));

      PostgresTest.postgresClient.commit();

      assertEquals(0, successor.getFromId());
      assertEquals(newLineageGraphVersionId, successor.getToId());
    } finally {
      PostgresTest.postgresClient.commit();
    }
  }
}
