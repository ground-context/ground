package edu.berkeley.ground.api.models.gremlin;

import org.junit.Test;

import edu.berkeley.ground.api.GremlinTest;
import edu.berkeley.ground.api.models.Structure;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class GremlinStructureFactoryTest extends GremlinTest {

  public GremlinStructureFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testStructureCreation() throws GroundException {
    String testName = "test";
    GremlinStructureFactory structureFactory = (GremlinStructureFactory) super.factories.getStructureFactory();
    structureFactory.create(testName);

    Structure structure = structureFactory.retrieveFromDatabase(testName);

    assertEquals(testName, structure.getName());
    assertEquals("Structures." + testName, structure.getId());
  }
}
