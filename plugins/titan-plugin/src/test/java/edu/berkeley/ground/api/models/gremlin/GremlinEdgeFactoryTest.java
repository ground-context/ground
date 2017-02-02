package edu.berkeley.ground.api.models.gremlin;

import org.junit.Test;

import edu.berkeley.ground.api.GremlinTest;
import edu.berkeley.ground.api.models.Edge;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class GremlinEdgeFactoryTest extends GremlinTest {

  public GremlinEdgeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testEdgeCreation() {
    try {
      String testName = "test";
      GremlinEdgeFactory edgeFactory = (GremlinEdgeFactory) super.factories.getEdgeFactory();
      edgeFactory.create(testName);

      Edge edge = edgeFactory.retrieveFromDatabase(testName);

      assertEquals(testName, edge.getName());
      assertEquals("Edges." + testName, edge.getId());
    } catch (GroundException ge) {
      fail(ge.getMessage());
    }
  }
}
