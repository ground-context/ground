package edu.berkeley.ground.api.models.postgres;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import edu.berkeley.ground.api.PostgresTest;
import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.db.PostgresClient.PostgresConnection;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class PostgresRichVersionFactoryTest extends PostgresTest {

  public PostgresRichVersionFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testReference() throws GroundException {
    PostgresConnection connection = null;

    try {
      connection = super.cassandraClient.getConnection();
      long id = 1;
      String testReference = "http://www.google.com";
      Map<String, String> parameters = new HashMap<>();
      parameters.put("http", "GET");

      super.richVersionFactory.insertIntoDatabase(connection, id, new HashMap<>(), -1,
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
    PostgresConnection connection = null;

    try {
      connection = super.cassandraClient.getConnection();
      long id = 1;

      Map<String, Tag> tags = new HashMap<>();
      tags.put("justkey", new Tag(-1, "justkey", null, null));
      tags.put("withintvalue", new Tag(-1, "withintvalue", 1, GroundType.INTEGER));
      tags.put("withstringvalue", new Tag(-1, "withstringvalue", "1", GroundType.STRING));
      tags.put("withboolvalue", new Tag(-1, "withboolvalue", true, GroundType.BOOLEAN));

      super.richVersionFactory.insertIntoDatabase(connection, id, tags, -1,
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
    PostgresConnection connection = null;
    try {
      connection = super.cassandraClient.getConnection();
      long id = 2;

      String structureName = "testStructure";
      long structureId = super.factories.getStructureFactory().create(structureName).getId();

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
    PostgresConnection connection = null;

    try {
      long structureVersionId = -1;
      long id = 1;

      // none of these operations should fail
      try {
        connection = super.cassandraClient.getConnection();

        String structureName = "testStructure";
        long structureId = super.factories.getStructureFactory().create(structureName).getId();

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
      super.richVersionFactory.insertIntoDatabase(connection, id, tags, structureVersionId, null,
          new HashMap<>());
    } finally {
      connection.abort();
    }
  }
}
