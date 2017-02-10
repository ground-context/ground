package edu.berkeley.ground.api.models.neo4j;

import org.junit.Test;

import edu.berkeley.ground.api.Neo4jTest;
import edu.berkeley.ground.api.models.Structure;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class Neo4jStructureFactoryTest extends Neo4jTest {

  public Neo4jStructureFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testStructureCreation() throws GroundException {
    String testName = "test";
    Neo4jStructureFactory edgeFactory = (Neo4jStructureFactory) super.factories.getStructureFactory();
    edgeFactory.create(testName);

    Structure edge = edgeFactory.retrieveFromDatabase(testName);

    assertEquals(testName, edge.getName());
  }
}
