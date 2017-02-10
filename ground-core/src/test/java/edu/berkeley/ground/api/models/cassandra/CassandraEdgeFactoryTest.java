package edu.berkeley.ground.api.models.cassandra;

import org.junit.Test;

import edu.berkeley.ground.api.CassandraTest;
import edu.berkeley.ground.api.models.Edge;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class CassandraEdgeFactoryTest extends CassandraTest {

  public CassandraEdgeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testEdgeCreation() throws GroundException {
    String testName = "test";
    CassandraEdgeFactory edgeFactory = (CassandraEdgeFactory) super.factories.getEdgeFactory();
    edgeFactory.create(testName);

    Edge edge = edgeFactory.retrieveFromDatabase(testName);

    assertEquals(testName, edge.getName());
  }
}
