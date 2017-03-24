package edu.berkeley.ground.dao.models.neo4j;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.berkeley.ground.dao.Neo4jTest;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class Neo4jTagFactoryTest extends Neo4jTest {

  public Neo4jTagFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testGetItemIdsByTag() throws GroundException {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

    long nodeId1 = super.factories.getNodeFactory().create("test1", null, tagsMap).getId();
    long nodeId2 = super.factories.getNodeFactory().create("test2", null, tagsMap).getId();

    List<Long> ids = super.tagFactory.getItemIdsByTag("testtag");

    super.neo4jClient.commit();

    assertTrue(ids.contains(nodeId1));
    assertTrue(ids.contains(nodeId2));
  }

  @Test
  public void testGetVersionIdsByTag() throws GroundException {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

    long nodeId = super.factories.getNodeFactory().create("test1", null, new HashMap<>()).getId();

    long nodeVersionId1 = super.factories.getNodeVersionFactory().create(tagsMap,
        -1, null, new HashMap<>(), nodeId, new ArrayList<>()).getId();
    long nodeVersionId2 = super.factories.getNodeVersionFactory().create(tagsMap,
        -1, null, new HashMap<>(), nodeId, new ArrayList<>()).getId();

    List<Long> ids = super.tagFactory.getVersionIdsByTag("testtag");

    super.neo4jClient.commit();

    assertTrue(ids.contains(nodeVersionId1));
    assertTrue(ids.contains(nodeVersionId2));
  }
}
