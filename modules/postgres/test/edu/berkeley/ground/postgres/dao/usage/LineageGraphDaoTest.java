package edu.berkeley.ground.postgres.dao.usage;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.usage.LineageGraph;
import edu.berkeley.ground.common.model.version.VersionHistoryDag;
import edu.berkeley.ground.common.model.version.VersionSuccessor;
import edu.berkeley.ground.postgres.dao.PostgresTest;

import org.junit.Test;

import javax.sound.sampled.Line;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


  import static org.junit.Assert.*;

public class LineageGraphDaoTest extends PostgresTest {

  public LineageGraphDaoTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageGraphCreation() throws GroundException {
    try {
      String testName = "test";
      String sourceKey = "testKey";

      LineageGraph lineageGraph = new LineageGraph(0L, testName, sourceKey, new HashMap<>());
      PostgresTest.lineageGraphDao.create(lineageGraph);
      LineageGraph graph = PostgresTest.lineageGraphDao.retrieveFromDatabase(sourceKey);

      assertEquals(testName, graph.getName());
      assertEquals(sourceKey, graph.getSourceKey());
    } finally {
      //PostgresTest.postgresClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadLineageGraph() throws GroundException {
    String sourceKey = "test";

    try {
      PostgresTest.lineageGraphDao.retrieveFromDatabase(sourceKey);
    } catch (GroundException e) {
      assertEquals(GroundException.class, e.getClass());

      throw e;
    } finally {
      //PostgresTest.postgresClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testCreateDuplicateLineageGraph() throws GroundException {
    String lineageGraphName = "lineageGraphName";
    String lineageGraphKey = "lineageGraphKey";

    try {
      try {
        LineageGraph lineageGraph = new LineageGraph(0L,lineageGraphName, lineageGraphKey, new HashMap<>());
        PostgresTest.lineageGraphDao.create(lineageGraph);
      } catch (GroundException e) {
        fail(e.getMessage());
      }
      LineageGraph duplicateGraph = new LineageGraph(0L,lineageGraphName, lineageGraphKey, new HashMap<>());
      PostgresTest.lineageGraphDao.create(duplicateGraph);
    } finally {
      //PostgresTest.postgresClient.commit();
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

      PostgresTest.lineageGraphDao.truncate(lineageGraphId, 1);

      VersionHistoryDag<?> dag = PostgresTest.versionHistoryDagDao
        .retrieveFromDatabase(lineageGraphId);

      assertEquals(1, dag.getEdgeIds().size());

      VersionSuccessor<?> successor = PostgresTest.versionSuccessorDao.retrieveFromDatabase(
        dag.getEdgeIds().get(0));

      //PostgresTest.postgresClient.commit();

      assertEquals(0, successor.getFromId());
      assertEquals(newLineageGraphVersionId, successor.getToId());
    } finally {
      //PostgresTest.postgresClient.commit();
    }
  }
}
