package edu.berkeley.ground.api.models.neo4j;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import edu.berkeley.ground.api.Neo4jTest;
import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class Neo4jStructureVersionFactoryTest extends Neo4jTest {

  public Neo4jStructureVersionFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testStructureVersionCreation() throws GroundException {
    String structureName = "testStructure";
    long structureId = super.factories.getStructureFactory().create(structureName).getId();

    Map<String, GroundType> structureVersionAttributes = new HashMap<>();
    structureVersionAttributes.put("intfield", GroundType.INTEGER);
    structureVersionAttributes.put("boolfield", GroundType.BOOLEAN);
    structureVersionAttributes.put("strfield", GroundType.STRING);

    long structureVersionId = super.factories.getStructureVersionFactory().create(
        structureId, structureVersionAttributes, new ArrayList<>()).getId();

    StructureVersion retrieved = super.factories.getStructureVersionFactory()
        .retrieveFromDatabase(structureVersionId);

    assertEquals(structureId, retrieved.getStructureId());
    Map<String, GroundType> retrievedAttributes = retrieved.getAttributes();

    for (String key : structureVersionAttributes.keySet()) {
      assert (retrievedAttributes).containsKey(key);
      assertEquals(structureVersionAttributes.get(key), retrievedAttributes.get(key));
    }
  }
}
