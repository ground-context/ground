package edu.berkeley.ground.api.usage.neo4j;

import org.junit.Test;

import edu.berkeley.ground.api.Neo4jTest;
import edu.berkeley.ground.api.usage.LineageEdge;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class Neo4jLineageEdgeFactoryTest extends Neo4jTest {

  public Neo4jLineageEdgeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageEdgeCreation() throws GroundException {
    String testName = "test";
    Neo4jLineageEdgeFactory edgeFactory = (Neo4jLineageEdgeFactory) super.factories.getLineageEdgeFactory();
    edgeFactory.create(testName);

    LineageEdge edge = edgeFactory.retrieveFromDatabase(testName);

    assertEquals(testName, edge.getName());
  }
}
