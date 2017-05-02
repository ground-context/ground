package dao.models;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import exceptions.GroundException;
import models.models.StructureVersion;
import models.models.Tag;
import models.versions.GroundType;

import static org.junit.Assert.assertEquals;

public class RichVersionFactoryTest {

  @Test(expected = GroundException.class)
  public void testSpecifyEmptyTags() throws GroundException {
    Map<String, GroundType> attributes = new HashMap<>();
    attributes.put("test", GroundType.STRING);

    StructureVersion structureVersion = new StructureVersion(1, 1, attributes);

    try {
      RichVersionFactory.checkStructureTags(structureVersion, new HashMap<>());
    } catch (GroundException e) {
      assertEquals("No tags were specified", e.getMessage());

      throw e;
    }
  }

  @Test(expected = GroundException.class)
  public void testEmptyValue() throws GroundException {
    Map<String, GroundType> attributes = new HashMap<>();
    attributes.put("test", GroundType.STRING);

    StructureVersion structureVersion = new StructureVersion(1, 1, attributes);

    Map<String, Tag> tags = new HashMap<>();
    tags.put("test", new Tag(1, -1, "test", null, null));

    try {
      RichVersionFactory.checkStructureTags(structureVersion, tags);
    } catch (GroundException e) {
      assertEquals("Tag with key test did not have a value.", e.getMessage());

      throw e;
    }
  }

  @Test(expected = GroundException.class)
  public void testWrongValue() throws GroundException {
    Map<String, GroundType> attributes = new HashMap<>();
    attributes.put("test", GroundType.STRING);

    StructureVersion structureVersion = new StructureVersion(1, 1, attributes);

    Map<String, Tag> tags = new HashMap<>();
    tags.put("test", new Tag(1, -1, "test", 1, GroundType.INTEGER));

    try {
      RichVersionFactory.checkStructureTags(structureVersion, tags);
    } catch (GroundException e) {
      assertEquals("Tag with key test did not have a value of the correct type: expected [string]" +
          " but found [integer].", e.getMessage());

      throw e;
    }
  }
}
