package edu.berkeley.ground.api.models.cassandra;

import org.junit.Test;

import edu.berkeley.ground.api.CassandraTest;
import edu.berkeley.ground.api.models.Structure;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class CassandraStructureFactoryTest extends CassandraTest {

  public CassandraStructureFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testStructureCreation() {
    try {
      String testName = "test";
      CassandraStructureFactory edgeFactory = (CassandraStructureFactory) super.factories.getStructureFactory();
      edgeFactory.create(testName);

      Structure edge = edgeFactory.retrieveFromDatabase(testName);

      assertEquals(testName, edge.getName());
    } catch (GroundException ge) {
      fail(ge.getMessage());
    }
  }
}
