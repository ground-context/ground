package edu.berkeley.ground.dao.models.cassandra;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import edu.berkeley.ground.model.CassandraTest;
import edu.berkeley.ground.model.models.RichVersion;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class CassandraRichVersionFactoryTest extends CassandraTest {

  public CassandraRichVersionFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testReference() throws GroundException {
    try {
      long id = 1;
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
      super.cassandraClient.abort();
    }
  }

  @Test
  public void testTags() throws GroundException {
    try {
      long id = 1;

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
      super.cassandraClient.abort();
    }
  }

  @Test
  public void testStructureVersionConformation() throws GroundException {
    try {
      long id = 1;

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
      super.cassandraClient.abort();
    }
  }

  @Test(expected = GroundException.class)
  public void testStructureVersionFails() throws GroundException {
    try {
      long structureVersionId = -1;
      long id = 1;

      // none of these operations should fail
      try {
        String structureName = "testStructure";
        long structureId = super.factories.getStructureFactory().create(structureName, new HashMap<>()).getId();

        Map<String, GroundType> structureVersionAttributes = new HashMap<>();
        structureVersionAttributes.put("intfield", GroundType.INTEGER);
        structureVersionAttributes.put("boolfield", GroundType.BOOLEAN);
        structureVersionAttributes.put("strfield", GroundType.STRING);

        structureVersionId = super.factories.getStructureVersionFactory().create(structureId, structureVersionAttributes, new ArrayList<>()).getId();
      } catch (GroundException ge) {
        super.cassandraClient.abort();

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
      super.cassandraClient.abort();
    }
  }
}
