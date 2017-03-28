package edu.berkeley.ground.dao.models;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.StructureVersion;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;

public class RichVersionFactoryTest {

  @Test(expected = GroundException.class)
  public void testSpecifyEmptyTags() throws GroundException {
    Map<String, GroundType> attributes = new HashMap<>();
    attributes.put("test", GroundType.STRING);

    StructureVersion structureVersion = new StructureVersion(1, 1, attributes);

    RichVersionFactory.checkStructureTags(structureVersion, new HashMap<>());
  }

  @Test(expected = GroundException.class)
  public void testEmptyValue() throws GroundException {
    Map<String, GroundType> attributes = new HashMap<>();
    attributes.put("test", GroundType.STRING);

    StructureVersion structureVersion = new StructureVersion(1, 1, attributes);

    Map<String, Tag> tags = new HashMap<>();
    tags.put("test", new Tag(1, "test", null, null));

    RichVersionFactory.checkStructureTags(structureVersion, tags);
  }
}
