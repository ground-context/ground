package edu.berkeley.ground.dao.models.neo4j;

import org.junit.Test;

import java.util.HashMap;

import edu.berkeley.ground.dao.Neo4jTest;
import edu.berkeley.ground.model.models.Graph;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class Neo4jGraphFactoryTest extends Neo4jTest {

  public Neo4jGraphFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testGraphCreation() throws GroundException {
    String testName = "test";
    String sourceKey = "testKey";

    Neo4jGraphFactory edgeFactory = (Neo4jGraphFactory) super.factories.getGraphFactory();
    edgeFactory.create(testName, sourceKey, new HashMap<>());

    Graph graph = edgeFactory.retrieveFromDatabase(testName);

    assertEquals(testName, graph.getName());
    assertEquals(sourceKey, graph.getSourceKey());
  }
}
