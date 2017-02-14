package edu.berkeley.ground.api.usage.cassandra;

import org.junit.Test;

import edu.berkeley.ground.api.CassandraTest;
import edu.berkeley.ground.api.usage.LineageEdge;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class CassandraLineageEdgeFactoryTest extends CassandraTest {

  public CassandraLineageEdgeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageEdgeCreation() throws GroundException {
    String testName = "test";
    CassandraLineageEdgeFactory edgeFactory = (CassandraLineageEdgeFactory) super.factories.getLineageEdgeFactory();
    edgeFactory.create(testName);

    LineageEdge edge = edgeFactory.retrieveFromDatabase(testName);

    assertEquals(testName, edge.getName());
  }
}
