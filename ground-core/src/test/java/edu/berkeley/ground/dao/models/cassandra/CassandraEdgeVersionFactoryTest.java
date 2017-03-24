package edu.berkeley.ground.dao.models.cassandra;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.berkeley.ground.dao.CassandraTest;
import edu.berkeley.ground.model.models.EdgeVersion;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class CassandraEdgeVersionFactoryTest extends CassandraTest {

  public CassandraEdgeVersionFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testEdgeVersionCreation() throws GroundException {
    String firstTestNode = "firstTestNode";
    long firstTestNodeId = CassandraTest.factories.getNodeFactory().create(firstTestNode, null,
        new HashMap<>()).getId();
    long firstNodeVersionId = CassandraTest.factories.getNodeVersionFactory().create(new HashMap<>(),
        -1, null, new HashMap<>(), firstTestNodeId, new ArrayList<>()).getId();

    String secondTestNode = "secondTestNode";
    long secondTestNodeId = CassandraTest.factories.getNodeFactory().create(secondTestNode, null,
        new HashMap<>()).getId();
    long secondNodeVersionId = CassandraTest.factories.getNodeVersionFactory().create(new HashMap<>(),
        -1, null, new HashMap<>(), secondTestNodeId, new ArrayList<>()).getId();

    String edgeName = "testEdge";
    long edgeId = CassandraTest.factories.getEdgeFactory().create(edgeName, null, firstTestNodeId,
        secondTestNodeId, new HashMap<>()).getId();

    String structureName = "testStructure";
    long structureId = CassandraTest.factories.getStructureFactory().create(structureName, null,
        new HashMap<>()).getId();

    Map<String, GroundType> structureVersionAttributes = new HashMap<>();
    structureVersionAttributes.put("intfield", GroundType.INTEGER);
    structureVersionAttributes.put("boolfield", GroundType.BOOLEAN);
    structureVersionAttributes.put("strfield", GroundType.STRING);

    long structureVersionId = CassandraTest.factories.getStructureVersionFactory().create(
        structureId, structureVersionAttributes, new ArrayList<>()).getId();

    Map<String, Tag> tags = new HashMap<>();
    tags.put("intfield", new Tag(-1, "intfield", 1, GroundType.INTEGER));
    tags.put("strfield", new Tag(-1, "strfield", "1", GroundType.STRING));
    tags.put("boolfield", new Tag(-1, "boolfield", true, GroundType.BOOLEAN));

    String testReference = "http://www.google.com";
    Map<String, String> parameters = new HashMap<>();
    parameters.put("http", "GET");

    long edgeVersionId = CassandraTest.factories.getEdgeVersionFactory().create(tags,
        structureVersionId, testReference, parameters, edgeId, firstNodeVersionId, -1,
        secondNodeVersionId, -1, new ArrayList<>()).getId();

    EdgeVersion retrieved = CassandraTest.factories.getEdgeVersionFactory().retrieveFromDatabase(edgeVersionId);

    assertEquals(edgeId, retrieved.getEdgeId());
    assertEquals(structureVersionId, retrieved.getStructureVersionId());
    assertEquals(testReference, retrieved.getReference());
    assertEquals(firstNodeVersionId, retrieved.getFromNodeVersionStartId());
    assertEquals(secondNodeVersionId, retrieved.getToNodeVersionStartId());
    assertEquals(-1, retrieved.getFromNodeVersionEndId());
    assertEquals(-1, retrieved.getToNodeVersionEndId());

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

  @Test
  public void testCorrectEndVersion() throws GroundException {
    String firstTestNode = "firstTestNode";
    long firstTestNodeId = CassandraTest.factories.getNodeFactory().create(firstTestNode, null,
        new HashMap<>()).getId();
    long firstNodeVersionId = CassandraTest.factories.getNodeVersionFactory().create(new HashMap<>(),
        -1, null, new HashMap<>(), firstTestNodeId, new ArrayList<>()).getId();

    String secondTestNode = "secondTestNode";
    long secondTestNodeId = CassandraTest.factories.getNodeFactory().create(secondTestNode, null,
        new HashMap<>()).getId();
    long secondNodeVersionId = CassandraTest.factories.getNodeVersionFactory().create(new HashMap<>(),
        -1, null, new HashMap<>(), secondTestNodeId, new ArrayList<>()).getId();

    String edgeName = "testEdge";
    long edgeId = CassandraTest.factories.getEdgeFactory().create(edgeName, null, firstTestNodeId,
        secondTestNodeId, new HashMap<>()).getId();


    long edgeVersionId = CassandraTest.factories.getEdgeVersionFactory().create(new HashMap<>(),
        -1, null, new HashMap<>(), edgeId, firstNodeVersionId, -1, secondNodeVersionId, -1,
        new ArrayList<>()).getId();

    EdgeVersion retrieved = CassandraTest.factories.getEdgeVersionFactory()
        .retrieveFromDatabase(edgeVersionId);

    assertEquals(edgeId, retrieved.getEdgeId());
    assertEquals(-1, retrieved.getStructureVersionId());
    assertEquals(null, retrieved.getReference());
    assertEquals(firstNodeVersionId, retrieved.getFromNodeVersionStartId());
    assertEquals(secondNodeVersionId, retrieved.getToNodeVersionStartId());
    assertEquals(-1, retrieved.getFromNodeVersionEndId());
    assertEquals(-1, retrieved.getToNodeVersionEndId());

    // create two new node versions in each of the nodes
    List<Long> parents = new ArrayList<>();
    parents.add(firstNodeVersionId);
    long fromEndId = CassandraTest.factories.getNodeVersionFactory().create(new HashMap<>(), -1,
        null, new HashMap<>(), firstTestNodeId, parents).getId();

    parents.clear();
    parents.add(fromEndId);
    long newFirstNodeVersionId = CassandraTest.factories.getNodeVersionFactory().create(new
        HashMap<>(), -1, null, new HashMap<>(), firstTestNodeId, parents).getId();

    parents.clear();
    parents.add(secondNodeVersionId);
    long toEndId = CassandraTest.factories.getNodeVersionFactory().create(new HashMap<>(), -1, null,
        new HashMap<>(), secondTestNodeId, parents).getId();

    parents.clear();
    parents.add(toEndId);
    long newSecondNodeVersionId = CassandraTest.factories.getNodeVersionFactory().create(new
        HashMap<>(), -1, null, new HashMap<>(), secondTestNodeId, parents).getId();

    parents.clear();
    parents.add(edgeVersionId);
    long newEdgeVersionId = CassandraTest.factories.getEdgeVersionFactory().create(new HashMap<>(),
        -1, null, new HashMap<>(), edgeId, newFirstNodeVersionId, -1, newSecondNodeVersionId, -1,
        parents).getId();

    EdgeVersion parent = CassandraTest.factories.getEdgeVersionFactory()
        .retrieveFromDatabase(edgeVersionId);
    EdgeVersion child = CassandraTest.factories.getEdgeVersionFactory()
        .retrieveFromDatabase(newEdgeVersionId);

    assertEquals(edgeId, child.getEdgeId());
    assertEquals(-1, child.getStructureVersionId());
    assertEquals(null, child.getReference());
    assertEquals(newFirstNodeVersionId, child.getFromNodeVersionStartId());
    assertEquals(newSecondNodeVersionId, child.getToNodeVersionStartId());
    assertEquals(-1, child.getFromNodeVersionEndId());
    assertEquals(-1, child.getToNodeVersionEndId());

    // Make sure that the end versions were set correctly
    assertEquals(firstNodeVersionId, parent.getFromNodeVersionStartId());
    assertEquals(secondNodeVersionId, parent.getToNodeVersionStartId());
    assertEquals(fromEndId, parent.getFromNodeVersionEndId());
    assertEquals(toEndId, parent.getToNodeVersionEndId());
  }
}
