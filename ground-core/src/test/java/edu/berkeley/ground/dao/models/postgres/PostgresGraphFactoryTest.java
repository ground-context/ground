package edu.berkeley.ground.dao.models.postgres;

import org.junit.Test;

import java.util.HashMap;

import edu.berkeley.ground.model.PostgresTest;
import edu.berkeley.ground.model.models.Graph;
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
    edgeFactory.create(testName, new HashMap<>());

    Graph edge = edgeFactory.retrieveFromDatabase(testName);

    assertEquals(testName, edge.getName());
  }
}
