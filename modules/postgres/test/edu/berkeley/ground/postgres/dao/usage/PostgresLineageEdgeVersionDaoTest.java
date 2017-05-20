package edu.berkeley.ground.postgres.dao.usage;

import static org.junit.Assert.assertEquals;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.usage.LineageEdgeVersion;
import edu.berkeley.ground.common.model.version.Tag;
import edu.berkeley.ground.postgres.dao.PostgresTest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class PostgresLineageEdgeVersionDaoTest extends PostgresTest {

  public PostgresLineageEdgeVersionDaoTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageEdgeVersionCreation() throws GroundException {
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

    LineageEdgeVersion lineageEdgeVersion = new LineageEdgeVersion(0L, tags, structureVersionId,
                                                                    testReference, parameters, firstNodeVersionId,
                                                                    secondNodeVersionId, lineageEdgeId);

    long lineageEdgeVersionId = PostgresTest.lineageEdgeVersionDao
                                  .create(lineageEdgeVersion, new ArrayList<>()).getId();

    LineageEdgeVersion retrieved = PostgresTest.lineageEdgeVersionDao
                                     .retrieveFromDatabase(lineageEdgeVersionId);

    assertEquals(lineageEdgeId, retrieved.getLineageEdgeId());
    assertEquals(structureVersionId, (long) retrieved.getStructureVersionId());
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
  }

  @Test(expected = GroundException.class)
  public void testBadLineageEdgeVersion() throws GroundException {
    long id = 1;

    try {
      PostgresTest.lineageEdgeVersionDao.retrieveFromDatabase(id);
    } catch (GroundException e) {
      assertEquals(GroundException.class, e.getClass());

      throw e;
    }
  }
}
