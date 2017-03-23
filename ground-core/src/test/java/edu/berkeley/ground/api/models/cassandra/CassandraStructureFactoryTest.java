package edu.berkeley.ground.api.models.cassandra;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
      CassandraStructureFactory edgeFactory = (CassandraStructureFactory) CassandraTest.factories.getStructureFactory();
      edgeFactory.create(testName, new HashMap<>());

      Structure edge = edgeFactory.retrieveFromDatabase(testName);

      assertEquals(testName, edge.getName());
    } catch (GroundException ge) {
      fail(ge.getMessage());
    }
  }

  @Test
  public void testLeafRetrieval() throws GroundException {
    String structureName = "testStructure1";
    long structureId = CassandraTest.factories.getStructureFactory().create(structureName, new HashMap<>()).getId();

    long structureVersionId = CassandraTest.factories.getStructureVersionFactory().create(structureId,
        new HashMap<>(), new ArrayList<>()).getId();
    long secondNVId = CassandraTest.factories.getStructureVersionFactory().create(structureId,
        new HashMap<>(), new ArrayList<>()).getId();

    List<Long> leaves = CassandraTest.factories.getStructureFactory().getLeaves(structureName);

    assertTrue(leaves.contains(structureVersionId));
    assertTrue(leaves.contains(secondNVId));
  }
}
