package edu.berkeley.ground.api.models.gremlin;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.berkeley.ground.api.GremlinTest;
import edu.berkeley.ground.api.models.GraphVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class GremlinGraphVersionFactoryTest extends GremlinTest {

  public GremlinGraphVersionFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testGraphVersionCreation() {
    try {
      String firstTestNode = "firstTestNode";
      String firstTestNodeId = super.factories.getNodeFactory().create(firstTestNode).getId();
      String firstNodeVersionId = super.factories.getNodeVersionFactory().create(new HashMap<>(),
          null, null, new HashMap<>(), firstTestNodeId, new ArrayList<>()).getId();

      String secondTestNode = "secondTestNode";
      String secondTestNodeId = super.factories.getNodeFactory().create(secondTestNode).getId();
      String secondNodeVersionId = super.factories.getNodeVersionFactory().create(new HashMap<>(),
          null, null, new HashMap<>(), secondTestNodeId, new ArrayList<>()).getId();

      String edgeName = "testEdge";
      String edgeId = super.factories.getEdgeFactory().create(edgeName).getId();
      String edgeVersionId = super.factories.getEdgeVersionFactory().create(new HashMap<>(),
          null, null, new HashMap<>(), edgeId, firstNodeVersionId, secondNodeVersionId,
          new ArrayList<>()).getId();

      List<String> edgeVersionIds = new ArrayList<>();
      edgeVersionIds.add(edgeVersionId);

      String graphId = super.factories.getGraphFactory().create("testGraph").getId();

      String structureName = "testStructure";
      String structureId = super.factories.getStructureFactory().create(structureName).getId();

      Map<String, GroundType> structureVersionAttributes = new HashMap<>();
      structureVersionAttributes.put("intfield", GroundType.INTEGER);
      structureVersionAttributes.put("boolfield", GroundType.BOOLEAN);
      structureVersionAttributes.put("strfield", GroundType.STRING);

      String structureVersionId = super.factories.getStructureVersionFactory().create(
          structureId, structureVersionAttributes, new ArrayList<>()).getId();

      Map<String, Tag> tags = new HashMap<>();
      tags.put("intfield", new Tag(null, "intfield", 1, GroundType.INTEGER));
      tags.put("strfield", new Tag(null, "strfield", "1", GroundType.STRING));
      tags.put("boolfield", new Tag(null, "boolfield", true, GroundType.BOOLEAN));

      String testReference = "http://www.google.com";
      Map<String, String> parameters = new HashMap<>();
      parameters.put("http", "GET");

      String graphVersionId = super.factories.getGraphVersionFactory().create(tags,
          structureVersionId, testReference, parameters, graphId, edgeVersionIds,
          new ArrayList<>()).getId();

      GraphVersion retrieved = super.factories.getGraphVersionFactory().retrieveFromDatabase(graphVersionId);

      assertEquals(graphId, retrieved.getGraphId());
      assertEquals(structureVersionId, retrieved.getStructureVersionId());
      assertEquals(testReference, retrieved.getReference());
      assertEquals(edgeVersionIds.size(), retrieved.getEdgeVersionIds().size());

      List<String> retrievedEdgeVersionIds = retrieved.getEdgeVersionIds();

      for (String id : edgeVersionIds) {
        assert (retrievedEdgeVersionIds).contains(id);
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
    } catch (GroundException ge) {
      fail(ge.getMessage());
    }
  }
}
