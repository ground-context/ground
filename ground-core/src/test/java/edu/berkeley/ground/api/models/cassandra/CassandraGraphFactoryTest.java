package edu.berkeley.ground.api.models.cassandra;

import org.junit.Test;

import edu.berkeley.ground.api.CassandraTest;
import edu.berkeley.ground.api.models.Graph;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class CassandraGraphFactoryTest extends CassandraTest {

  public CassandraGraphFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testGraphCreation() {
    try {
      String testName = "test";
      CassandraGraphFactory edgeFactory = (CassandraGraphFactory) super.factories.getGraphFactory();
      edgeFactory.create(testName);

      Graph edge = edgeFactory.retrieveFromDatabase(testName);

      assertEquals(testName, edge.getName());
    } catch (GroundException ge) {
      fail(ge.getMessage());
    }
  }
}
