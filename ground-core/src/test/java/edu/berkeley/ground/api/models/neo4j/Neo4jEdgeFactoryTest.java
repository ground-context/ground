package edu.berkeley.ground.api.models.neo4j;

import org.junit.Test;

import edu.berkeley.ground.api.Neo4jTest;
import edu.berkeley.ground.api.models.Edge;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class Neo4jEdgeFactoryTest extends Neo4jTest {

  public Neo4jEdgeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testEdgeCreation() throws GroundException {
    String testName = "test";
    Neo4jEdgeFactory edgeFactory = (Neo4jEdgeFactory) super.factories.getEdgeFactory();
    edgeFactory.create(testName);

    Edge edge = edgeFactory.retrieveFromDatabase(testName);

    assertEquals(testName, edge.getName());
  }
}
