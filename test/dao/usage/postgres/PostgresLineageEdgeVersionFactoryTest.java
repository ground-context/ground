package dao.usage.postgres;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import dao.PostgresTest;
import exceptions.GroundVersionNotFoundException;
import models.usage.LineageEdgeVersion;
import models.models.Tag;
import models.versions.GroundType;
import exceptions.GroundException;

import static org.junit.Assert.*;

public class PostgresLineageEdgeVersionFactoryTest extends PostgresTest {

  public PostgresLineageEdgeVersionFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageEdgeVersionCreation() throws GroundException {
    try {
      String firstTestNode = "firstTestNode";
      long firstTestNodeId = PostgresTest.createNode(firstTestNode).getId();
      long firstNodeVersionId = PostgresTest.createNodeVersion(firstTestNodeId).getId();

      String secondTestNode = "secondTestNode";
      long secondTestNodeId = PostgresTest.createNode(secondTestNode).getId();
      long secondNodeVersionId = PostgresTest.createNodeVersion(secondTestNodeId).getId();

      String lineageEdgeName = "testLineageEdge";
      long lineageEdgeId = PostgresTest.createLineageEdge(lineageEdgeName).getId();

      String structureName = "testStructure";
      long structureId = PostgresTest.createStructure(structureName).getId();
      long structureVersionId = PostgresTest.createStructureVersion(structureId).getId();

      Map<String, Tag> tags = PostgresTest.createTags();

      String testReference = "http://www.google.com";
      Map<String, String> parameters = new HashMap<>();
      parameters.put("http", "GET");

      long lineageEdgeVersionId = PostgresTest.lineageEdgeVersionFactory.create(
          tags, structureVersionId, testReference, parameters, firstNodeVersionId,
          secondNodeVersionId, lineageEdgeId, new ArrayList<>()).getId();

      LineageEdgeVersion retrieved = PostgresTest.lineageEdgeVersionFactory
          .retrieveFromDatabase(lineageEdgeVersionId);

      assertEquals(lineageEdgeId, retrieved.getLineageEdgeId());
      assertEquals(structureVersionId, retrieved.getStructureVersionId());
      assertEquals(testReference, retrieved.getReference());
      assertEquals(retrieved.getFromId(), firstNodeVersionId);
      assertEquals(retrieved.getToId(), secondNodeVersionId);

      assertEquals(parameters.size(), retrieved.getParameters().size());
      assertEquals(tags.size(), retrieved.getTags().size());

      Map<String, String> retrievedParameters = retrieved.getParameters();
      Map<String, Tag> retrievedTags = retrieved.getTags();

      for (String key : parameters.keySet()) {
        assert (retrievedParameters).containsKey(key);
        assertEquals(parameters.get(key), retrievedParameters.get(key));
      }

      for (String key : tags.keySet()) {
        assert (retrievedTags).containsKey(key);
        assertEquals(tags.get(key), retrievedTags.get(key));
      }
    } finally {
      PostgresTest.postgresClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testBadLineageEdgeVersion() throws GroundException {
    long id = 1;

    try {
      PostgresTest.lineageEdgeVersionFactory.retrieveFromDatabase(id);
    } catch (GroundException e) {
      assertEquals(GroundVersionNotFoundException.class, e.getClass());

      throw e;
    } finally {
      PostgresTest.postgresClient.commit();
    }
  }

}
