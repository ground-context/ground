package edu.berkeley.ground.api.models.neo4j;

import org.junit.Test;

import edu.berkeley.ground.api.Neo4jTest;
import edu.berkeley.ground.api.models.Graph;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class Neo4jGraphFactoryTest extends Neo4jTest {

  public Neo4jGraphFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testGraphCreation() throws GroundException {
    String testName = "test";
    Neo4jGraphFactory edgeFactory = (Neo4jGraphFactory) super.factories.getGraphFactory();
    edgeFactory.create(testName);

    Graph edge = edgeFactory.retrieveFromDatabase(testName);

    assertEquals(testName, edge.getName());
  }
}
