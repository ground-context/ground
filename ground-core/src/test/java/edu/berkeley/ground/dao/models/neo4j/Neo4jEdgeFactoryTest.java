package edu.berkeley.ground.dao.models.neo4j;

import org.junit.Test;

import java.util.HashMap;

import edu.berkeley.ground.model.Neo4jTest;
import edu.berkeley.ground.model.models.Edge;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class Neo4jEdgeFactoryTest extends Neo4jTest {

  public Neo4jEdgeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testEdgeCreation() throws GroundException {
    String testName = "test";

    long firstNodeId = 1;
    long secondNodeId = 2;

    Neo4jEdgeFactory edgeFactory = (Neo4jEdgeFactory) super.factories.getEdgeFactory();
    edgeFactory.create(testName, firstNodeId, secondNodeId, new HashMap<>());

    Edge edge = edgeFactory.retrieveFromDatabase(testName);

    assertEquals(testName, edge.getName());
    assertEquals(firstNodeId, edge.getFromNodeId());
    assertEquals(secondNodeId, edge.getToNodeId());
  }
}
