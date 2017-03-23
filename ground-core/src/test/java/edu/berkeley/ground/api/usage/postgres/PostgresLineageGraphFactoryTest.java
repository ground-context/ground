package edu.berkeley.ground.api.usage.postgres;

import org.junit.Test;

import java.util.HashMap;

import edu.berkeley.ground.api.PostgresTest;
import edu.berkeley.ground.api.usage.LineageGraph;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.assertEquals;

public class PostgresLineageGraphFactoryTest extends PostgresTest {

  public PostgresLineageGraphFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testGraphCreation() throws GroundException {
    String testName = "test";
    PostgresLineageGraphFactory lineageGraphFactory = (PostgresLineageGraphFactory) super.factories
        .getLineageGraphFactory();

    lineageGraphFactory.create(testName, new HashMap<>());

    LineageGraph lineageGraph = lineageGraphFactory.retrieveFromDatabase(testName);

    assertEquals(testName, lineageGraph.getName());
  }
}
