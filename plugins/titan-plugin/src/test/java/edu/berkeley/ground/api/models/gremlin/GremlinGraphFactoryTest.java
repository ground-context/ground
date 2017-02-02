package edu.berkeley.ground.api.models.gremlin;

import org.junit.Test;

import edu.berkeley.ground.api.GremlinTest;
import edu.berkeley.ground.api.models.Graph;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class GremlinGraphFactoryTest extends GremlinTest {

  public GremlinGraphFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testGraphCreation() {
    try {
      String testName = "test";
      GremlinGraphFactory edgeFactory = (GremlinGraphFactory) super.factories.getGraphFactory();
      edgeFactory.create(testName);

      Graph edge = edgeFactory.retrieveFromDatabase(testName);

      assertEquals(testName, edge.getName());
      assertEquals("Graphs." + testName, edge.getId());
    } catch (GroundException ge) {
      fail(ge.getMessage());
    }
  }
}
