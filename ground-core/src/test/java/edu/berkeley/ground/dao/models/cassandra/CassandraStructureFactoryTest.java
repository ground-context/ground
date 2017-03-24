package edu.berkeley.ground.dao.models.cassandra;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.berkeley.ground.dao.CassandraTest;
import edu.berkeley.ground.model.models.Structure;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class CassandraStructureFactoryTest extends CassandraTest {

  public CassandraStructureFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testStructureCreation() throws GroundException {
    String testName = "test";
    String sourceKey = "testKey";

    CassandraStructureFactory edgeFactory = (CassandraStructureFactory) CassandraTest.factories.getStructureFactory();
    edgeFactory.create(testName, sourceKey, new HashMap<>());

    Structure structure = edgeFactory.retrieveFromDatabase(testName);

    assertEquals(testName, structure.getName());
    assertEquals(sourceKey, structure.getSourceKey());
  }

  @Test
  public void testLeafRetrieval() throws GroundException {
    String structureName = "testStructure1";
    String sourceKey = "testKey";

    long structureId = CassandraTest.factories.getStructureFactory().create(structureName,
        sourceKey, new HashMap<>()).getId();

    long structureVersionId = CassandraTest.factories.getStructureVersionFactory().create(structureId,
        new HashMap<>(), new ArrayList<>()).getId();
    long secondNVId = CassandraTest.factories.getStructureVersionFactory().create(structureId,
        new HashMap<>(), new ArrayList<>()).getId();

    List<Long> leaves = CassandraTest.factories.getStructureFactory().getLeaves(structureName);

    assertTrue(leaves.contains(structureVersionId));
    assertTrue(leaves.contains(secondNVId));
  }
}
