package edu.berkeley.ground.dao.usage.postgres;

import org.junit.Test;

import java.util.HashMap;

import edu.berkeley.ground.dao.PostgresTest;
import edu.berkeley.ground.model.usage.LineageGraph;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.assertEquals;

public class PostgresLineageGraphFactoryTest extends PostgresTest {

  public PostgresLineageGraphFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testGraphCreation() throws GroundException {
    String testName = "test";
    String sourceKey = "testKey";

    PostgresLineageGraphFactory lineageGraphFactory = (PostgresLineageGraphFactory) super.factories
        .getLineageGraphFactory();
    lineageGraphFactory.create(testName, sourceKey, new HashMap<>());

    LineageGraph lineageGraph = lineageGraphFactory.retrieveFromDatabase(testName);

    assertEquals(testName, lineageGraph.getName());
    assertEquals(sourceKey, lineageGraph.getSourceKey());
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadLineageGraph() throws GroundException {
    String testName = "test";

    try {
      super.factories.getLineageGraphFactory().retrieveFromDatabase(testName);
    } catch (GroundException e) {
      assertEquals("No LineageGraph found with name " + testName + ".", e.getMessage());

      throw e;
    }
  }
}
