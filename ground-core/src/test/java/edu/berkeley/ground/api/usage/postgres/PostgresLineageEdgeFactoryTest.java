package edu.berkeley.ground.api.usage.postgres;

import org.junit.Test;

import edu.berkeley.ground.api.PostgresTest;
import edu.berkeley.ground.api.usage.LineageEdge;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class PostgresLineageEdgeFactoryTest extends PostgresTest {

  public PostgresLineageEdgeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageEdgeCreation() throws GroundException {
    String testName = "test";
    PostgresLineageEdgeFactory edgeFactory = (PostgresLineageEdgeFactory) super.factories.getLineageEdgeFactory();
    edgeFactory.create(testName);

    LineageEdge edge = edgeFactory.retrieveFromDatabase(testName);

    assertEquals(testName, edge.getName());
  }
}
