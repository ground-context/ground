package edu.berkeley.ground.dao.models.cassandra;

import org.junit.Test;

import java.util.HashMap;

import edu.berkeley.ground.dao.CassandraTest;
import edu.berkeley.ground.model.models.Graph;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class CassandraGraphFactoryTest extends CassandraTest {

  public CassandraGraphFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testGraphCreation() throws GroundException {
    String testName = "test";
    String sourceKey = "testKey";

    CassandraGraphFactory graphFactory = (CassandraGraphFactory) super.factories.getGraphFactory();
    graphFactory.create(testName, sourceKey, new HashMap<>());

    Graph graph = graphFactory.retrieveFromDatabase(testName);

    assertEquals(testName, graph.getName());
    assertEquals(sourceKey, graph.getSourceKey());
  }
}
