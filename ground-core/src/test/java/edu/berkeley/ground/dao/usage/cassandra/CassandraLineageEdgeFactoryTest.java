package edu.berkeley.ground.dao.usage.cassandra;

import org.junit.Test;

import java.util.HashMap;

import edu.berkeley.ground.model.CassandraTest;
import edu.berkeley.ground.model.usage.LineageEdge;
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
    edgeFactory.create(testName, new HashMap<>());

    LineageEdge edge = edgeFactory.retrieveFromDatabase(testName);

    assertEquals(testName, edge.getName());
  }
}
