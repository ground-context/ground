package edu.berkeley.ground.postgres.dao.usage;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.usage.LineageEdge;
import edu.berkeley.ground.common.model.version.VersionHistoryDag;
import edu.berkeley.ground.common.model.version.VersionSuccessor;
import edu.berkeley.ground.postgres.dao.PostgresTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;

public class LineageEdgeDaoTest extends PostgresTest {

  public LineageEdgeDaoTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageEdgeCreation() throws GroundException {
    try {
      String testName = "test";
      String sourceKey = "testKey";

      LineageEdge insertLineageEdge = new LineageEdge(0L, testName, sourceKey, new HashMap<>());
      PostgresTest.lineageEdgeDao.create(insertLineageEdge);
      LineageEdge lineageEdge = PostgresTest.lineageEdgeDao.retrieveFromDatabase(sourceKey);

      assertEquals(testName, lineageEdge.getName());
      assertEquals(sourceKey, lineageEdge.getSourceKey());
    } finally {
      //PostgresTest.postgresClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadLineageEdge() throws GroundException {
    String sourceKey = "test";

    try {
      PostgresTest.lineageEdgeDao.retrieveFromDatabase(sourceKey);
    } catch (GroundException e) {
      assertEquals(GroundException.class, e.getClass());

      throw e;
    } finally {
      //PostgresTest.postgresClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testCreateDuplicateLineageEdge() throws GroundException {
    String lineageEdgeName = "lineageEdgeName";
    String lineageEdgeKey = "lineageEdgeKey";

    try {
      try {
        LineageEdge lineageEdge = new LineageEdge(0L, lineageEdgeName, lineageEdgeKey, new HashMap<>());
        PostgresTest.lineageEdgeDao.create(lineageEdge);
      } catch (GroundException e) {
        fail(e.getMessage());
      }

      LineageEdge duplicateEdge = new LineageEdge(0L, lineageEdgeName, lineageEdgeKey, new HashMap<>());
      PostgresTest.lineageEdgeDao.create(duplicateEdge);
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

      List<Long> parents = new ArrayList<>();
      parents.add(lineageEdgeVersionId);
      long newLineageEdgeVersionId = PostgresTest.createLineageEdgeVersion(lineageEdgeId,
        firstNodeVersionId, secondNodeVersionId, parents).getId();

      PostgresTest.lineageEdgeDao.truncate(lineageEdgeId, 1);

      VersionHistoryDag<?> dag = PostgresTest.versionHistoryDagDao
        .retrieveFromDatabase(lineageEdgeId);

      assertEquals(1, dag.getEdgeIds().size());

      VersionSuccessor<?> successor = PostgresTest.versionSuccessorDao.retrieveFromDatabase(
        dag.getEdgeIds().get(0));

      //PostgresTest.postgresClient.commit();

      assertEquals(0, successor.getFromId());
      assertEquals(newLineageEdgeVersionId, successor.getToId());
    } finally {
      //PostgresTest.postgresClient.commit();
    }
  }
}
