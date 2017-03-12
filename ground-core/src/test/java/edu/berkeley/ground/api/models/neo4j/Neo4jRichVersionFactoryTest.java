package edu.berkeley.ground.api.models.neo4j;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import edu.berkeley.ground.api.Neo4jTest;
import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class Neo4jRichVersionFactoryTest extends Neo4jTest {

  public Neo4jRichVersionFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testReference() throws GroundException {
    try {
      /* Create a NodeVersion because Neo4j's rich version factory looks for an existing version with this id */
      long testNodeId = super.factories.getNodeFactory().create("testNode", new HashMap<>()).getId();
      long id = super.createNodeVersion(testNodeId);

      String testReference = "http://www.google.com";
      Map<String, String> parameters = new HashMap<>();
      parameters.put("http", "GET");

      super.richVersionFactory.insertIntoDatabase(id, new HashMap<>(), -1, testReference, parameters);

      RichVersion retrieved = super.richVersionFactory.retrieveFromDatabase(id);

      assertEquals(id, retrieved.getId());
      assertEquals(testReference, retrieved.getReference());

      Map<String, String> retrievedParams = retrieved.getParameters();
      for (String key : parameters.keySet()) {
        assert (retrievedParams).containsKey(key);
        assertEquals(parameters.get(key), retrievedParams.get(key));
      }
    } finally {
      super.neo4jClient.abort();
    }
  }

  @Test
  public void testTags() throws GroundException {
    try {
      /* Create a NodeVersion because Neo4j's rich version factory looks for an existing version with this id */
      long testNodeId = super.factories.getNodeFactory().create("testNode", new HashMap<>()).getId();
      long id = super.createNodeVersion(testNodeId);

      Map<String, Tag> tags = new HashMap<>();
      tags.put("justkey", new Tag(-1, "justkey", null, null));
      tags.put("withintvalue", new Tag(-1, "withintvalue", 1, GroundType.INTEGER));
      tags.put("withstringvalue", new Tag(-1, "withstringvalue", "1", GroundType.STRING));
      tags.put("withboolvalue", new Tag(-1, "withboolvalue", true, GroundType.BOOLEAN));

      super.richVersionFactory.insertIntoDatabase(id, tags, -1, null, new HashMap<>());

      RichVersion retrieved = super.richVersionFactory.retrieveFromDatabase(id);

      assertEquals(id, retrieved.getId());
      assertEquals(tags.size(), retrieved.getTags().size());

      Map<String, Tag> retrievedTags = retrieved.getTags();
      for (String key : tags.keySet()) {
        assert (retrievedTags).containsKey(key);
        assertEquals(tags.get(key), retrievedTags.get(key));
        assertEquals(retrieved.getId(), retrievedTags.get(key).getId());
      }
    } finally {
      super.neo4jClient.abort();
    }
  }

  @Test
  public void testStructureVersionConformation() throws GroundException {
    try {
      /* Create a NodeVersion because Neo4j's rich version factory looks for an existing * version with this id */
      long testNodeId = super.factories.getNodeFactory().create("testNode", new HashMap<>()).getId();
      long id = super.createNodeVersion(testNodeId);

      String structureName = "testStructure";
      long structureId = super.factories.getStructureFactory().create(structureName, new HashMap<>()).getId();

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

      super.richVersionFactory.insertIntoDatabase(id, tags, structureVersionId, null,
          new HashMap<>());

      RichVersion retrieved = super.richVersionFactory.retrieveFromDatabase(id);
      assertEquals(retrieved.getStructureVersionId(), structureVersionId);
    } finally {
      super.neo4jClient.abort();
    }
  }

  @Test(expected = GroundException.class)
  public void testStructureVersionFails() throws GroundException {
    long structureVersionId = -1;

    try {
      /* Create a NodeVersion because Neo4j's rich version factory looks for an existing version with this id */
      long testNodeId = super.factories.getNodeFactory().create("testNode", new HashMap<>()).getId();
      long id = super.createNodeVersion(testNodeId);

      // none of these operations should fail
      try {
        String structureName = "testStructure";
        long structureId = super.factories.getStructureFactory().create(structureName, new HashMap<>()).getId();

        Map<String, GroundType> structureVersionAttributes = new HashMap<>();
        structureVersionAttributes.put("intfield", GroundType.INTEGER);
        structureVersionAttributes.put("boolfield", GroundType.BOOLEAN);
        structureVersionAttributes.put("strfield", GroundType.STRING);

        structureVersionId = super.factories.getStructureVersionFactory().create(
            structureId, structureVersionAttributes, new ArrayList<>()).getId();
      } catch (GroundException ge) {
        fail(ge.getMessage());
      }

      Map<String, Tag> tags = new HashMap<>();
      tags.put("intfield", new Tag(-1, "intfield", 1, GroundType.INTEGER));
      tags.put("intfield", new Tag(-1, "strfield", "1", GroundType.STRING));
      tags.put("intfield", new Tag(-1, "boolfield", true, GroundType.BOOLEAN));

      // this should fail
      super.richVersionFactory.insertIntoDatabase(id, tags, structureVersionId, null,
          new HashMap<>());
    } finally {
      super.neo4jClient.abort();
    }
  }
}
