package edu.berkeley.ground.dao.usage.cassandra;

import org.junit.Test;

import java.util.HashMap;

import edu.berkeley.ground.model.CassandraTest;
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
    CassandraLineageGraphFactory graphFactory = (CassandraLineageGraphFactory) super.factories
        .getLineageGraphFactory();
    graphFactory.create(testName, new HashMap<>());

    LineageGraph graph = graphFactory.retrieveFromDatabase(testName);

    assertEquals(testName, graph.getName());
  }
}
