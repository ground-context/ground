package edu.berkeley.ground.api.models.postgres;

import org.junit.Test;

import edu.berkeley.ground.api.PostgresTest;
import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class PostgresNodeFactoryTest extends PostgresTest {

  public PostgresNodeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testNodeCreation() throws GroundException {
    String testName = "test";
    PostgresNodeFactory nodeFactory = (PostgresNodeFactory) super.factories.getNodeFactory();
    nodeFactory.create(testName);

    Node node = nodeFactory.retrieveFromDatabase(testName);

    assertEquals(testName, node.getName());
  }
}
