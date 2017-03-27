package edu.berkeley.ground.dao.usage.postgres;

import org.junit.Test;

import java.util.HashMap;

import edu.berkeley.ground.dao.PostgresTest;
import edu.berkeley.ground.model.usage.LineageEdge;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class PostgresLineageEdgeFactoryTest extends PostgresTest {

  public PostgresLineageEdgeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageEdgeCreation() throws GroundException {
    String testName = "test";
    String sourceKey = "testKey";

    PostgresLineageEdgeFactory lineageEdgeFactory = (PostgresLineageEdgeFactory)
        super.factories.getLineageEdgeFactory();
    lineageEdgeFactory.create(testName, sourceKey, new HashMap<>());

    LineageEdge lineageEdge = lineageEdgeFactory.retrieveFromDatabase(testName);

    assertEquals(testName, lineageEdge.getName());
    assertEquals(sourceKey, lineageEdge.getSourceKey());
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadLineageEdge() throws GroundException {
    String testName = "test";

    try {
      super.factories.getLineageEdgeFactory().retrieveFromDatabase(testName);
    } catch (GroundException e) {
      assertEquals("No LineageEdge found with name " + testName + ".", e.getMessage());

      throw e;
    }
  }
}
