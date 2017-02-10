package edu.berkeley.ground.api.models.cassandra;

import org.junit.Test;

import edu.berkeley.ground.api.CassandraTest;
import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class CassandraNodeFactoryTest extends CassandraTest {

  public CassandraNodeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testNodeCreation() {
    try {
      String testName = "test";
      CassandraNodeFactory edgeFactory = (CassandraNodeFactory) super.factories.getNodeFactory();
      edgeFactory.create(testName);

      Node edge = edgeFactory.retrieveFromDatabase(testName);

      assertEquals(testName, edge.getName());
    } catch (GroundException ge) {
      fail(ge.getMessage());
    }
  }
}
