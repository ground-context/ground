package edu.berkeley.ground.dao.usage.neo4j;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.berkeley.ground.dao.Neo4jTest;
import edu.berkeley.ground.exceptions.GroundVersionNotFoundException;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.usage.LineageGraphVersion;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Neo4jLineageGraphVersionFactoryTest extends Neo4jTest {

  public Neo4jLineageGraphVersionFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageGraphVersionCreation() throws GroundException {
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

    String structureName = "testStructure";
    long structureId = Neo4jTest.createStructure(structureName).getId();
    long structureVersionId = Neo4jTest.createStructureVersion(structureId).getId();

    Map<String, Tag> tags = Neo4jTest.createTags();

    String testReference = "http://www.google.com";
    Map<String, String> parameters = new HashMap<>();
    parameters.put("http", "GET");

    long lineageGraphVersionId = Neo4jTest.lineageGraphsResource
        .createLineageGraphVersion(lineageGraphId, tags, parameters, structureVersionId,
            testReference, lineageEdgeVersionIds, new ArrayList<>()).getId();

    LineageGraphVersion retrieved = Neo4jTest.lineageGraphsResource
        .getLineageGraphVersion(lineageGraphVersionId);

    assertEquals(lineageGraphId, retrieved.getLineageGraphId());
    assertEquals(structureVersionId, retrieved.getStructureVersionId());
    assertEquals(testReference, retrieved.getReference());
    assertEquals(lineageEdgeVersionIds.size(), retrieved.getLineageEdgeVersionIds().size());

    List<Long> retrievedLineageEdgeVersionIds = retrieved.getLineageEdgeVersionIds();

    for (long id : lineageEdgeVersionIds) {
      assert (retrievedLineageEdgeVersionIds).contains(id);
    }

    assertEquals(parameters.size(), retrieved.getParameters().size());
    assertEquals(tags.size(), retrieved.getTags().size());

    Map<String, String> retrievedParameters = retrieved.getParameters();
    Map<String, Tag> retrievedTags = retrieved.getTags();

    for (String key : parameters.keySet()) {
      assert (retrievedParameters).containsKey(key);
      assertEquals(parameters.get(key), retrievedParameters.get(key));
    }

    for (String key : tags.keySet()) {
      assert (retrievedTags).containsKey(key);
      assertEquals(tags.get(key), retrievedTags.get(key));
    }
  }

  @Test(expected = GroundException.class)
  public void testBadLineageGraphVersion() throws GroundException {
    long id = 1;

    try {
      Neo4jTest.lineageGraphsResource.getLineageGraphVersion(id);
    } catch (GroundException e) {
      assertEquals(GroundVersionNotFoundException.class, e.getClass());

      throw e;
    }
  }

  @Test
  public void testCreateEmptyLineageGraph() throws GroundException {
    String lineageGraphName = "testGraph";
    long lineageGraphId = Neo4jTest.createLineageGraph(lineageGraphName).getId();

    long lineageGraphVersionId = Neo4jTest.createLineageGraphVersion(lineageGraphId,
        new ArrayList<>()).getId();

    LineageGraphVersion retrieved = Neo4jTest.lineageGraphsResource
        .getLineageGraphVersion(lineageGraphVersionId);

    assertTrue(retrieved.getLineageEdgeVersionIds().isEmpty());
  }
}
