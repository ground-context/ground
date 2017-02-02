package edu.berkeley.ground.api.models.gremlin;

import org.junit.Test;

import edu.berkeley.ground.api.GremlinTest;
import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class GremlinNodeFactoryTest extends GremlinTest {

  public GremlinNodeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testNodeCreation() throws GroundException {
    String testName = "test";
    GremlinNodeFactory nodeFactory = (GremlinNodeFactory) super.factories.getNodeFactory();
    nodeFactory.create(testName).getId();

    Node node = nodeFactory.retrieveFromDatabase(testName);

    assertEquals(testName, node.getName());
    assertEquals("Nodes." + testName, node.getId());
  }
}
