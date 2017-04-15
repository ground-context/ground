package edu.berkeley.ground.dao.usage.postgres;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.berkeley.ground.dao.PostgresTest;
import edu.berkeley.ground.exceptions.GroundItemNotFoundException;
import edu.berkeley.ground.model.usage.LineageEdge;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.versions.VersionHistoryDag;
import edu.berkeley.ground.model.versions.VersionSuccessor;

import static org.junit.Assert.*;

public class PostgresLineageEdgeFactoryTest extends PostgresTest {

  public PostgresLineageEdgeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageEdgeCreation() throws GroundException {
    String testName = "test";
    String sourceKey = "testKey";

    PostgresTest.lineageEdgesResource.createLineageEdge(testName, sourceKey, new HashMap<>());
    LineageEdge lineageEdge = PostgresTest.lineageEdgesResource.getLineageEdge(sourceKey);

    assertEquals(testName, lineageEdge.getName());
    assertEquals(sourceKey, lineageEdge.getSourceKey());
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadLineageEdge() throws GroundException {
    String sourceKey = "test";

    try {
      PostgresTest.lineageEdgesResource.getLineageEdge(sourceKey);
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
      PostgresTest.lineageEdgesResource.createLineageEdge(lineageEdgeName, lineageEdgeKey, new HashMap<>());
    } catch (GroundException e) {
      fail(e.getMessage());
    }

    PostgresTest.lineageEdgesResource.createLineageEdge(lineageEdgeName, lineageEdgeKey, new HashMap<>());
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

    List<Long> parents = new ArrayList<>();
    parents.add(lineageEdgeVersionId);
    long newLineageEdgeVersionId = PostgresTest.createLineageEdgeVersion(lineageEdgeId,
        firstNodeVersionId, secondNodeVersionId, parents).getId();

    PostgresTest.lineageEdgesResource.truncateLineageEdge(lineageEdgeName, 1);

    VersionHistoryDag<?> dag = PostgresTest.versionHistoryDAGFactory
        .retrieveFromDatabase(lineageEdgeId);

    assertEquals(1, dag.getEdgeIds().size());

    VersionSuccessor<?> successor = PostgresTest.versionSuccessorFactory.retrieveFromDatabase(
        dag.getEdgeIds().get(0));

    PostgresTest.postgresClient.commit();

    assertEquals(0, successor.getFromId());
    assertEquals(newLineageEdgeVersionId, successor.getToId());
  }
}
