package edu.berkeley.ground.dao.models.postgres;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.berkeley.ground.dao.PostgresTest;
import edu.berkeley.ground.model.models.Structure;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class PostgresStructureFactoryTest extends PostgresTest {

  public PostgresStructureFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testStructureCreation() throws GroundException {
    String testName = "test";
    String sourceKey = "testKey";

    PostgresStructureFactory structureFactory = (PostgresStructureFactory) super.factories
        .getStructureFactory();
    structureFactory.create(testName, sourceKey, new HashMap<>());

    Structure structure = structureFactory.retrieveFromDatabase(testName);

    assertEquals(testName, structure.getName());
    assertEquals(sourceKey, structure.getSourceKey());
  }

  @Test
  public void testLeafRetrieval() throws GroundException {
    String structureName = "testStructure1";
    long structureId = super.factories.getStructureFactory().create(structureName, null,
        new HashMap<>()).getId();

    long structureVersionId = super.factories.getStructureVersionFactory().create(structureId,
        new HashMap<>(), new ArrayList<>()).getId();
    long secondNVId = super.factories.getStructureVersionFactory().create(structureId,
        new HashMap<>(), new ArrayList<>()).getId();

    List<Long> leaves = super.factories.getStructureFactory().getLeaves(structureName);

    assertTrue(leaves.contains(structureVersionId));
    assertTrue(leaves.contains(secondNVId));
  }
}
