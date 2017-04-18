package edu.berkeley.ground.dao.usage.postgres;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.berkeley.ground.dao.PostgresTest;
import edu.berkeley.ground.exceptions.GroundVersionNotFoundException;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.usage.LineageGraphVersion;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PostgresLineageGraphVersionFactoryTest extends PostgresTest {

  public PostgresLineageGraphVersionFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageGraphVersionCreation() throws GroundException {
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

    String structureName = "testStructure";
    long structureId = PostgresTest.createStructure(structureName).getId();
    long structureVersionId = PostgresTest.createStructureVersion(structureId).getId();

    Map<String, Tag> tags = PostgresTest.createTags();

    String testReference = "http://www.google.com";
    Map<String, String> parameters = new HashMap<>();
    parameters.put("http", "GET");

    long lineageGraphVersionId = PostgresTest.lineageGraphsResource
        .createLineageGraphVersion(lineageGraphId, tags, parameters, structureVersionId,
            testReference, lineageEdgeVersionIds, new ArrayList<>()).getId();

    LineageGraphVersion retrieved = PostgresTest.lineageGraphsResource
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
      PostgresTest.lineageGraphsResource.getLineageGraphVersion(id);
    } catch (GroundException e) {
      assertEquals(GroundVersionNotFoundException.class, e.getClass());

      throw e;
    }
  }

  @Test
  public void testCreateEmptyLineageGraph() throws GroundException {
    String lineageGraphName = "testGraph";
    long lineageGraphId = PostgresTest.createLineageGraph(lineageGraphName).getId();

    long lineageGraphVersionId = PostgresTest.createLineageGraphVersion(lineageGraphId,
        new ArrayList<>()).getId();

    LineageGraphVersion retrieved = PostgresTest.lineageGraphsResource
        .getLineageGraphVersion(lineageGraphVersionId);

    assertTrue(retrieved.getLineageEdgeVersionIds().isEmpty());
  }
}
