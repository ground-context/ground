package edu.berkeley.ground.api.models.neo4j;

import org.junit.Test;

import edu.berkeley.ground.api.Neo4jTest;
import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class Neo4jNodeFactoryTest extends Neo4jTest {

  public Neo4jNodeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testNodeCreation() throws GroundException {
    String testName = "test";
    Neo4jNodeFactory nodeFactory = (Neo4jNodeFactory) super.factories.getNodeFactory();
    nodeFactory.create(testName);

    Node node = nodeFactory.retrieveFromDatabase(testName);

    assertEquals(testName, node.getName());
  }
}
