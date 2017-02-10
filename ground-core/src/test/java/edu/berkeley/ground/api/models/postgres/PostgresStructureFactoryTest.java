package edu.berkeley.ground.api.models.postgres;

import org.junit.Test;

import edu.berkeley.ground.api.PostgresTest;
import edu.berkeley.ground.api.models.Structure;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class PostgresStructureFactoryTest extends PostgresTest {

  public PostgresStructureFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testStructureCreation() throws GroundException {
    String testName = "test";
    PostgresStructureFactory edgeFactory = (PostgresStructureFactory) super.factories.getStructureFactory();
    edgeFactory.create(testName);

    Structure edge = edgeFactory.retrieveFromDatabase(testName);

    assertEquals(testName, edge.getName());
  }
}
