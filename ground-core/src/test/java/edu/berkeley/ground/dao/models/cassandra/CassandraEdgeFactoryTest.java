package edu.berkeley.ground.dao.models.cassandra;

import org.junit.Test;

import java.util.HashMap;

import edu.berkeley.ground.dao.CassandraTest;
import edu.berkeley.ground.model.models.Edge;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class CassandraEdgeFactoryTest extends CassandraTest {

  public CassandraEdgeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testEdgeCreation() throws GroundException {
    String testName = "test";
    String sourceKey = "testKey";

    long fromNodeId = 1;
    long toNodeId = 2;

    CassandraEdgeFactory edgeFactory = (CassandraEdgeFactory) super.factories.getEdgeFactory();
    edgeFactory.create(testName, sourceKey, fromNodeId, toNodeId, new HashMap<>());

    Edge edge = edgeFactory.retrieveFromDatabase(testName);

    assertEquals(testName, edge.getName());
    assertEquals(fromNodeId, edge.getFromNodeId());
    assertEquals(toNodeId, edge.getToNodeId());
    assertEquals(sourceKey, edge.getSourceKey());
  }
}
