package edu.berkeley.ground.dao.usage.cassandra;

import org.junit.Test;

import java.util.HashMap;

import edu.berkeley.ground.dao.CassandraTest;
import edu.berkeley.ground.model.usage.LineageGraph;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.assertEquals;

public class CassandraLineageGraphFactoryTest extends CassandraTest {

  public CassandraLineageGraphFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageGraphCreation() throws GroundException {
    String testName = "test";
    String sourceKey = "testKey";

    CassandraLineageGraphFactory graphFactory = (CassandraLineageGraphFactory)
        CassandraTest.factories.getLineageGraphFactory();
    graphFactory.create(testName, sourceKey, new HashMap<>());

    LineageGraph graph = graphFactory.retrieveFromDatabase(testName);

    assertEquals(testName, graph.getName());
    assertEquals(sourceKey, graph.getSourceKey());
  }
}
