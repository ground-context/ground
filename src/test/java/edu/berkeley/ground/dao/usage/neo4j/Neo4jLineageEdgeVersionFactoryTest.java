package edu.berkeley.ground.dao.usage.neo4j;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import edu.berkeley.ground.dao.Neo4jTest;
import edu.berkeley.ground.exceptions.GroundVersionNotFoundException;
import edu.berkeley.ground.model.usage.LineageEdgeVersion;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class Neo4jLineageEdgeVersionFactoryTest extends Neo4jTest {

  public Neo4jLineageEdgeVersionFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageEdgeVersionCreation() throws GroundException {
    String firstTestNode = "firstTestNode";
    long firstTestNodeId = Neo4jTest.createNode(firstTestNode).getId();
    long firstNodeVersionId = Neo4jTest.createNodeVersion(firstTestNodeId).getId();

    String secondTestNode = "secondTestNode";
    long secondTestNodeId = Neo4jTest.createNode(secondTestNode).getId();
    long secondNodeVersionId = Neo4jTest.createNodeVersion(secondTestNodeId).getId();

    String lineageEdgeName = "testLineageEdge";
    long lineageEdgeId = Neo4jTest.createLineageEdge(lineageEdgeName).getId();

    String structureName = "testStructure";
    long structureId = Neo4jTest.createStructure(structureName).getId();
    long structureVersionId = Neo4jTest.createStructureVersion(structureId).getId();

    String testReference = "http://www.google.com";
    Map<String, String> parameters = new HashMap<>();
    parameters.put("http", "GET");

    Map<String, Tag> tags = Neo4jTest.createTags();

    long lineageEdgeVersionId = Neo4jTest.lineageEdgesResource.createLineageEdgeVersion(
        lineageEdgeId, tags, parameters, structureVersionId, testReference, firstNodeVersionId,
        secondNodeVersionId, new ArrayList<>()).getId();

    LineageEdgeVersion retrieved = Neo4jTest.lineageEdgesResource
        .getLineageEdgeVersion(lineageEdgeVersionId);

    assertEquals(lineageEdgeId, retrieved.getLineageEdgeId());
    assertEquals(structureVersionId, retrieved.getStructureVersionId());
    assertEquals(testReference, retrieved.getReference());
    assertEquals(retrieved.getFromId(), firstNodeVersionId);
    assertEquals(retrieved.getToId(), secondNodeVersionId);

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
  public void testBadLineageEdgeVersion() throws GroundException {
    long id = 1;

    try {
      Neo4jTest.lineageEdgesResource.getLineageEdgeVersion(id);
    } catch (GroundException e) {
      assertEquals(GroundVersionNotFoundException.class, e.getClass());

      throw e;
    }
  }
}
