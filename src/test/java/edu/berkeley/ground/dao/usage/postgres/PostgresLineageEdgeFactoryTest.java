package edu.berkeley.ground.dao.usage.postgres;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.berkeley.ground.dao.PostgresTest;
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

    PostgresLineageEdgeFactory lineageEdgeFactory = (PostgresLineageEdgeFactory)
        super.factories.getLineageEdgeFactory();
    lineageEdgeFactory.create(testName, sourceKey, new HashMap<>());

    LineageEdge lineageEdge = lineageEdgeFactory.retrieveFromDatabase(testName);

    assertEquals(testName, lineageEdge.getName());
    assertEquals(sourceKey, lineageEdge.getSourceKey());
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadLineageEdge() throws GroundException {
    String testName = "test";

    try {
      super.factories.getLineageEdgeFactory().retrieveFromDatabase(testName);
    } catch (GroundException e) {
      assertEquals("No LineageEdge found with name " + testName + ".", e.getMessage());

      throw e;
    }
  }

  @Test
  public void testTruncate() throws GroundException {
    String firstTestNode = "firstTestNode";
    long firstTestNodeId = super.factories.getNodeFactory().create(firstTestNode, null,
        new HashMap<>()).getId();
    long firstNodeVersionId = super.factories.getNodeVersionFactory().create(new HashMap<>(),
        -1, null, new HashMap<>(), firstTestNodeId, new ArrayList<>()).getId();

    String secondTestNode = "secondTestNode";
    long secondTestNodeId = super.factories.getNodeFactory().create(secondTestNode, null,
        new HashMap<>()).getId();
    long secondNodeVersionId = super.factories.getNodeVersionFactory().create(new HashMap<>(),
        -1, null, new HashMap<>(), secondTestNodeId, new ArrayList<>()).getId();

    String lineageEdgeName = "testLineageEdge";
    long lineageEdgeId = super.factories.getLineageEdgeFactory().create(lineageEdgeName, null,
        new HashMap<>()).getId();

    long lineageEdgeVersionId = super.factories.getLineageEdgeVersionFactory().create(
        new HashMap<>(), -1, null, new HashMap<>(), firstNodeVersionId, secondNodeVersionId,
        lineageEdgeId, new ArrayList<>()).getId();

    List<Long> parents = new ArrayList<>();
    parents.add(lineageEdgeVersionId);

    long newLineageEdgeVersionId = super.factories.getLineageEdgeVersionFactory().create(
        new HashMap<>(), -1, null, new HashMap<>(), firstNodeVersionId, secondNodeVersionId,
        lineageEdgeId, parents).getId();

    super.factories.getLineageEdgeFactory().truncate(lineageEdgeId, 1);

    VersionHistoryDag<?> dag = super.versionHistoryDAGFactory
        .retrieveFromDatabase(lineageEdgeId);

    assertEquals(1, dag.getEdgeIds().size());

    VersionSuccessor<?> successor = super.versionSuccessorFactory.retrieveFromDatabase(
        dag.getEdgeIds().get(0));

    super.postgresClient.commit();

    assertEquals(0, successor.getFromId());
    assertEquals(newLineageEdgeVersionId, successor.getToId());
  }
}
