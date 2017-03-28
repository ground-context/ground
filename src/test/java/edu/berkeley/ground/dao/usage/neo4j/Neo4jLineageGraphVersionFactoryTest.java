package edu.berkeley.ground.dao.usage.neo4j;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.berkeley.ground.dao.Neo4jTest;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.usage.LineageGraphVersion;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.assertEquals;

public class Neo4jLineageGraphVersionFactoryTest extends Neo4jTest {

  public Neo4jLineageGraphVersionFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageGraphVersionCreation() throws GroundException {
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

    String lineageEdgeName = "testEdge";
    long lineageEdgeId = super.factories.getLineageEdgeFactory().create(lineageEdgeName, null,
        new HashMap<>()).getId();
    long lineageEdgeVersionId = super.factories.getLineageEdgeVersionFactory().create(new HashMap<>(),
        -1, null, new HashMap<>(), lineageEdgeId, firstNodeVersionId, secondNodeVersionId, new ArrayList<>()).getId();

    List<Long> lineageEdgeVersionIds = new ArrayList<>();
    lineageEdgeVersionIds.add(lineageEdgeVersionId);

    long lineageGraphId = super.factories.getLineageGraphFactory().create("testGraph", null,
        new HashMap<>()).getId();

    String structureName = "testStructure";
    long structureId = super.factories.getStructureFactory().create(structureName, null,
        new HashMap<>()).getId();

    Map<String, GroundType> structureVersionAttributes = new HashMap<>();
    structureVersionAttributes.put("intfield", GroundType.INTEGER);
    structureVersionAttributes.put("boolfield", GroundType.BOOLEAN);
    structureVersionAttributes.put("strfield", GroundType.STRING);

    long structureVersionId = super.factories.getStructureVersionFactory().create(
        structureId, structureVersionAttributes, new ArrayList<>()).getId();

    Map<String, Tag> tags = new HashMap<>();
    tags.put("intfield", new Tag(-1, "intfield", 1, GroundType.INTEGER));
    tags.put("strfield", new Tag(-1, "strfield", "1", GroundType.STRING));
    tags.put("boolfield", new Tag(-1, "boolfield", true, GroundType.BOOLEAN));

    String testReference = "http://www.google.com";
    Map<String, String> parameters = new HashMap<>();
    parameters.put("http", "GET");

    long lineageGraphVersionId = super.factories.getLineageGraphVersionFactory().create(tags,
        structureVersionId, testReference, parameters, lineageGraphId, lineageEdgeVersionIds,
        new ArrayList<>()).getId();

    LineageGraphVersion retrieved = super.factories.getLineageGraphVersionFactory()
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
  }

  @Test(expected = GroundException.class)
  public void testBadLineageGraphVersion() throws GroundException {
    long id = 1;

    try {
      super.factories.getLineageGraphVersionFactory().retrieveFromDatabase(id);
    } catch (GroundException e) {
      assertEquals("No RichVersion found with id " + id + ".", e.getMessage());

      throw e;
    }
  }
}
