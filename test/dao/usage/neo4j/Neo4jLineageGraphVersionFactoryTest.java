package dao.usage.neo4j;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dao.Neo4jTest;
import exceptions.GroundVersionNotFoundException;
import models.models.Tag;
import models.usage.LineageGraphVersion;
import models.versions.GroundType;
import exceptions.GroundException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Neo4jLineageGraphVersionFactoryTest extends Neo4jTest {

  public Neo4jLineageGraphVersionFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageGraphVersionCreation() throws GroundException {
    try {
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

      long lineageGraphVersionId = Neo4jTest.lineageGraphVersionFactory
          .create(tags, structureVersionId, testReference, parameters, lineageGraphId,
              lineageEdgeVersionIds, new ArrayList<>()).getId();

      LineageGraphVersion retrieved = Neo4jTest.lineageGraphVersionFactory
          .retrieveFromDatabase(lineageGraphVersionId);

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
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testBadLineageGraphVersion() throws GroundException {
    long id = 1;

    try {
      Neo4jTest.lineageGraphVersionFactory.retrieveFromDatabase(id);
    } catch (GroundException e) {
      assertEquals(GroundVersionNotFoundException.class, e.getClass());

      throw e;
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }

  @Test
  public void testCreateEmptyLineageGraph() throws GroundException {
    String lineageGraphName = "testGraph";

    try {
      long lineageGraphId = Neo4jTest.createLineageGraph(lineageGraphName).getId();

      long lineageGraphVersionId = Neo4jTest.createLineageGraphVersion(lineageGraphId,
          new ArrayList<>()).getId();

      LineageGraphVersion retrieved = Neo4jTest.lineageGraphVersionFactory
          .retrieveFromDatabase(lineageGraphVersionId);

      assertTrue(retrieved.getLineageEdgeVersionIds().isEmpty());
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }
}
