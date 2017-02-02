package edu.berkeley.ground.api.usage.gremlin;

import org.junit.Test;

import edu.berkeley.ground.api.GremlinTest;
import edu.berkeley.ground.api.usage.LineageEdge;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class GremlinLineageEdgeFactoryTest extends GremlinTest {

  public GremlinLineageEdgeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageEdgeCreation() {
    try {
      String testName = "test";
      GremlinLineageEdgeFactory edgeFactory = (GremlinLineageEdgeFactory) super.factories.getLineageEdgeFactory();
      edgeFactory.create(testName);

      LineageEdge edge = edgeFactory.retrieveFromDatabase(testName);

      assertEquals(testName, edge.getName());
      assertEquals("LineageEdges." + testName, edge.getId());
    } catch (GroundException ge) {
      fail(ge.getMessage());
    }
  }
}
