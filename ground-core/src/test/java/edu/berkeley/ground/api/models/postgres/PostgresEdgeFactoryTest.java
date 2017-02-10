package edu.berkeley.ground.api.models.postgres;

import org.junit.Test;

import edu.berkeley.ground.api.PostgresTest;
import edu.berkeley.ground.api.models.Edge;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class PostgresEdgeFactoryTest extends PostgresTest {

  public PostgresEdgeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testEdgeCreation() throws GroundException {
    String testName = "test";
    PostgresEdgeFactory edgeFactory = (PostgresEdgeFactory) super.factories.getEdgeFactory();
    edgeFactory.create(testName);

    Edge edge = edgeFactory.retrieveFromDatabase(testName);

    assertEquals(testName, edge.getName());
  }
}
