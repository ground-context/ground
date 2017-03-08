package edu.berkeley.ground.api.models.neo4j;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.berkeley.ground.api.Neo4jTest;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.db.Neo4jClient.Neo4jConnection;
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

    long nodeId1 = super.factories.getNodeFactory().create("test1", tagsMap).getId();
    long nodeId2 = super.factories.getNodeFactory().create("test2", tagsMap).getId();

    Neo4jConnection connection = super.neo4jClient.getConnection();
    List<Long> ids = super.tagFactory.getItemIdsByTag(connection, "testtag");

    connection.commit();

    assertTrue(ids.contains(nodeId1));
    assertTrue(ids.contains(nodeId2));
  }

  @Test
  public void testGetVersionIdsByTag() throws GroundException {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

    long nodeId = super.factories.getNodeFactory().create("test1", new HashMap<>()).getId();

    long nodeVersionId1 = super.factories.getNodeVersionFactory().create(tagsMap,
        -1, null, new HashMap<>(), nodeId, new ArrayList<>()).getId();
    long nodeVersionId2 = super.factories.getNodeVersionFactory().create(tagsMap,
        -1, null, new HashMap<>(), nodeId, new ArrayList<>()).getId();

    Neo4jConnection connection = super.neo4jClient.getConnection();
    List<Long> ids = super.tagFactory.getVersionIdsByTag(connection, "testtag");

    connection.commit();

    assertTrue(ids.contains(nodeVersionId1));
    assertTrue(ids.contains(nodeVersionId2));
  }
}
