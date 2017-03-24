package edu.berkeley.ground.dao.models.neo4j;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.berkeley.ground.dao.Neo4jTest;
import edu.berkeley.ground.model.models.Structure;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class Neo4jStructureFactoryTest extends Neo4jTest {

  public Neo4jStructureFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testStructureCreation() throws GroundException {
    String testName = "test";
    String sourceKey = "testKey";

    Neo4jStructureFactory edgeFactory = (Neo4jStructureFactory) super.factories.getStructureFactory();
    edgeFactory.create(testName, sourceKey, new HashMap<>());

    Structure structure = edgeFactory.retrieveFromDatabase(testName);

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
