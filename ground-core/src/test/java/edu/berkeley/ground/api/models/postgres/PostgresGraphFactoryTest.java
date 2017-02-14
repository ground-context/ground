package edu.berkeley.ground.api.models.postgres;

import org.junit.Test;

import edu.berkeley.ground.api.PostgresTest;
import edu.berkeley.ground.api.models.Graph;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class PostgresGraphFactoryTest extends PostgresTest {

  public PostgresGraphFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testGraphCreation() throws GroundException {
    String testName = "test";
    PostgresGraphFactory edgeFactory = (PostgresGraphFactory) super.factories.getGraphFactory();
    edgeFactory.create(testName);

    Graph edge = edgeFactory.retrieveFromDatabase(testName);

    assertEquals(testName, edge.getName());
  }
}
