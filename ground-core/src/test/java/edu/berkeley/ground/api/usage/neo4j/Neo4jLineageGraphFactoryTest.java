package edu.berkeley.ground.api.usage.neo4j;

import org.junit.Test;

import java.util.HashMap;

import edu.berkeley.ground.api.Neo4jTest;
import edu.berkeley.ground.api.usage.LineageGraph;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.assertEquals;

public class Neo4jLineageGraphFactoryTest extends Neo4jTest {

  public Neo4jLineageGraphFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageGraphCreation() throws GroundException {
    String testName = "test";
    Neo4jLineageGraphFactory lineageGraphFactory = (Neo4jLineageGraphFactory) super.factories
        .getLineageGraphFactory();
    lineageGraphFactory.create(testName, new HashMap<>());

    LineageGraph lineageGraph = lineageGraphFactory.retrieveFromDatabase(testName);

    assertEquals(testName, lineageGraph.getName());
  }
}
