package dao.usage.postgres;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import dao.PostgresTest;
import exceptions.GroundItemNotFoundException;
import models.usage.LineageEdge;
import exceptions.GroundException;
import models.versions.VersionHistoryDag;
import models.versions.VersionSuccessor;

import static org.junit.Assert.*;

public class PostgresLineageEdgeFactoryTest extends PostgresTest {

  public PostgresLineageEdgeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageEdgeCreation() throws GroundException {
    try {
      String testName = "test";
      String sourceKey = "testKey";

      PostgresTest.lineageEdgeFactory.create(testName, sourceKey, new HashMap<>());
      LineageEdge lineageEdge = PostgresTest.lineageEdgeFactory.retrieveFromDatabase(sourceKey);

      assertEquals(testName, lineageEdge.getName());
      assertEquals(sourceKey, lineageEdge.getSourceKey());
    } finally {
      PostgresTest.postgresClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadLineageEdge() throws GroundException {
    String sourceKey = "test";

    try {
      PostgresTest.lineageEdgeFactory.retrieveFromDatabase(sourceKey);
    } catch (GroundException e) {
      assertEquals(GroundItemNotFoundException.class, e.getClass());

      throw e;
    } finally {
      PostgresTest.postgresClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testCreateDuplicateLineageEdge() throws GroundException {
    String lineageEdgeName = "lineageEdgeName";
    String lineageEdgeKey = "lineageEdgeKey";

    try {
      try {
        PostgresTest.lineageEdgeFactory.create(lineageEdgeName, lineageEdgeKey, new HashMap<>());
      } catch (GroundException e) {
        fail(e.getMessage());
      }

      PostgresTest.lineageEdgeFactory.create(lineageEdgeName, lineageEdgeKey, new HashMap<>());
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

      List<Long> parents = new ArrayList<>();
      parents.add(lineageEdgeVersionId);
      long newLineageEdgeVersionId = PostgresTest.createLineageEdgeVersion(lineageEdgeId,
          firstNodeVersionId, secondNodeVersionId, parents).getId();

      PostgresTest.lineageEdgeFactory.truncate(lineageEdgeId, 1);

      VersionHistoryDag<?> dag = PostgresTest.versionHistoryDAGFactory
          .retrieveFromDatabase(lineageEdgeId);

      assertEquals(1, dag.getEdgeIds().size());

      VersionSuccessor<?> successor = PostgresTest.versionSuccessorFactory.retrieveFromDatabase(
          dag.getEdgeIds().get(0));

      PostgresTest.postgresClient.commit();

      assertEquals(0, successor.getFromId());
      assertEquals(newLineageEdgeVersionId, successor.getToId());
    } finally {
      PostgresTest.postgresClient.commit();
    }
  }
}
