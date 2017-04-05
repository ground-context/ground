package edu.berkeley.ground.dao.usage.neo4j;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.berkeley.ground.dao.Neo4jTest;
import edu.berkeley.ground.model.usage.LineageGraph;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.versions.VersionHistoryDag;
import edu.berkeley.ground.model.versions.VersionSuccessor;

import static org.junit.Assert.assertEquals;

public class Neo4jLineageGraphFactoryTest extends Neo4jTest {

  public Neo4jLineageGraphFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageGraphCreation() throws GroundException {
    String testName = "test";
    String sourceKey = "testKey";

    Neo4jLineageGraphFactory lineageGraphFactory = (Neo4jLineageGraphFactory) super.factories
        .getLineageGraphFactory();
    lineageGraphFactory.create(testName, sourceKey, new HashMap<>());

    LineageGraph lineageGraph = lineageGraphFactory.retrieveFromDatabase(testName);

    assertEquals(testName, lineageGraph.getName());
    assertEquals(sourceKey, lineageGraph.getSourceKey());
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadLineageGraph() throws GroundException {
    String testName = "test";

    try {
      super.factories.getLineageGraphFactory().retrieveFromDatabase(testName);
    } catch (GroundException e) {
      assertEquals("No LineageGraph found with name " + testName + ".", e.getMessage());

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
    long lineageEdgeId = super.factories.getLineageEdgeFactory().create(lineageEdgeName,
        null, new HashMap<>()).getId();
    long lineageEdgeVersionId = super.factories.getLineageEdgeVersionFactory().create(
        new HashMap<>(), -1, null, new HashMap<>(), firstNodeVersionId, secondNodeVersionId,
        lineageEdgeId, new ArrayList<>()).getId();

    List<Long> lineageEdgeVersionIds = new ArrayList<>();
    lineageEdgeVersionIds.add(lineageEdgeVersionId);

    String lineageGraphName = "testLineageGraph";
    long lineageGraphId = super.factories.getLineageGraphFactory().create
        (lineageGraphName, null, new HashMap<>()).getId();

    long lineageGraphVersionId = super.factories.getLineageGraphVersionFactory().create(
        new HashMap<>(), -1, null, new HashMap<>(), lineageGraphId, lineageEdgeVersionIds,
        new ArrayList<>()).getId();

    List<Long> parents = new ArrayList<>();
    parents.add(lineageGraphVersionId);

    long newLineageGraphVersionId = super.factories.getLineageGraphVersionFactory().create(
        new HashMap<>(), -1, null, new HashMap<>(), lineageGraphId, lineageEdgeVersionIds,
        parents).getId();

    super.factories.getLineageGraphFactory().truncate(lineageGraphId, 1);

    VersionHistoryDag<?> dag = super.versionHistoryDAGFactory
        .retrieveFromDatabase(lineageGraphId);

    assertEquals(1, dag.getEdgeIds().size());

    VersionSuccessor<?> successor = super.versionSuccessorFactory.retrieveFromDatabase(
        dag.getEdgeIds().get(0));

    super.neo4jClient.commit();

    assertEquals(lineageGraphId, successor.getFromId());
    assertEquals(newLineageGraphVersionId, successor.getToId());
  }
}
