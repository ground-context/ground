package edu.berkeley.ground.api.models.gremlin;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import edu.berkeley.ground.api.GremlinTest;
import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.db.GremlinClient.GremlinConnection;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class GremlinRichVersionFactoryTest extends GremlinTest {

  public GremlinRichVersionFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testReference() throws GroundException {
    GremlinConnection connection = null;

    try {
      connection = super.gremlinClient.getConnection();

        /* Create a NodeVersion because Gremlin's rich version factory looks for an existing
         * version with this id */
      String nodeId = super.factories.getNodeFactory().create("testNodeId").getId();
      String id = super.createNodeVersion(nodeId);

      String testReference = "http://www.google.com";
      Map<String, String> parameters = new HashMap<>();
      parameters.put("http", "GET");

      super.richVersionFactory.insertIntoDatabase(connection, id, new HashMap<>(), null,
          testReference, parameters);

      RichVersion retrieved = super.richVersionFactory.retrieveFromDatabase(connection, id);

      assertEquals(id, retrieved.getId());
      assertEquals(testReference, retrieved.getReference());

      Map<String, String> retrievedParams = retrieved.getParameters();
      for (String key : parameters.keySet()) {
        assert (retrievedParams).containsKey(key);
        assertEquals(parameters.get(key), retrievedParams.get(key));
      }
    } finally {
      connection.abort();
    }
  }

  @Test
  public void testTags() throws GroundException {
    GremlinConnection connection = null;
    try {
      connection = super.gremlinClient.getConnection();

            /* Create a NodeVersion because Gremlin's rich version factory looks for an existing
             * version with this id */
      String nodeId = super.factories.getNodeFactory().create("testNodeId").getId();
      String id = super.createNodeVersion(nodeId);

      Map<String, Tag> tags = new HashMap<>();
      tags.put("justkey", new Tag(null, "justkey", null, null));
      tags.put("withintvalue", new Tag(null, "withintvalue", 1, GroundType.INTEGER));
      tags.put("withstringvalue", new Tag(null, "withstringvalue", "1", GroundType.STRING));
      tags.put("withboolvalue", new Tag(null, "withboolvalue", true, GroundType.BOOLEAN));

      super.richVersionFactory.insertIntoDatabase(connection, id, tags, null,
          null, new HashMap<>());

      RichVersion retrieved = super.richVersionFactory.retrieveFromDatabase(connection, id);

      assertEquals(id, retrieved.getId());
      assertEquals(tags.size(), retrieved.getTags().size());

      Map<String, Tag> retrievedTags = retrieved.getTags();
      for (String key : tags.keySet()) {
        assert (retrievedTags).containsKey(key);
        assertEquals(tags.get(key), retrievedTags.get(key));
        assertEquals(retrieved.getId(), retrievedTags.get(key).getVersionId());
      }
    } finally {
      connection.abort();
    }
  }

  @Test
  public void testStructureVersionConformation() throws GroundException {
    GremlinConnection connection = null;
    try {
      connection = super.gremlinClient.getConnection();

            /* Create a NodeVersion because Gremlin's rich version factory looks for an existing
             * version with this id */
      String nodeId = super.factories.getNodeFactory().create("testNodeId").getId();
      String id = super.createNodeVersion(nodeId);

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

      super.richVersionFactory.insertIntoDatabase(connection, id, tags, structureVersionId, null,
          new HashMap<>());

      RichVersion retrieved = super.richVersionFactory.retrieveFromDatabase(connection, id);
      assertEquals(retrieved.getStructureVersionId(), structureVersionId);
    } finally {
      connection.abort();
    }
  }

  @Test(expected = GroundException.class)
  public void testStructureVersionFails() throws GroundException {
    GremlinConnection connection = null;
    try {
      String structureVersionId = null;

            /* Create a NodeVersion because Gremlin's rich version factory looks for an existing
             * version with this id */
      String nodeId = super.factories.getNodeFactory().create("testNodeId").getId();
      String id = super.createNodeVersion(nodeId);

      // none of these operations should fail
      try {
        connection = super.gremlinClient.getConnection();

        String structureName = "testStructure";
        String structureId = super.factories.getStructureFactory().create(structureName).getId();

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
      tags.put("intfield", new Tag(null, "intfield", 1, GroundType.INTEGER));
      tags.put("intfield", new Tag(null, "strfield", "1", GroundType.STRING));
      tags.put("intfield", new Tag(null, "boolfield", true, GroundType.BOOLEAN));

      // this should fail
      super.richVersionFactory.insertIntoDatabase(connection, id, tags, structureVersionId, null,
          new HashMap<>());
    } finally {
      connection.abort();
    }
  }
}
