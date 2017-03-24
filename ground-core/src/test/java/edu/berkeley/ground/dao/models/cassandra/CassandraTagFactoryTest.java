package edu.berkeley.ground.dao.models.cassandra;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.berkeley.ground.dao.CassandraTest;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class CassandraTagFactoryTest extends CassandraTest {

  public CassandraTagFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testGetItemIdsByTag() throws GroundException {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

    long nodeId1 = CassandraTest.factories.getNodeFactory().create("test1", null, tagsMap).getId();
    long nodeId2 = CassandraTest.factories.getNodeFactory().create("test2", null, tagsMap).getId();

    List<Long> ids = CassandraTest.tagFactory.getItemIdsByTag("testtag");

    super.cassandraClient.commit();

    assertTrue(ids.contains(nodeId1));
    assertTrue(ids.contains(nodeId2));
  }

  @Test
  public void testGetVersionIdsByTag() throws GroundException {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

    long nodeId = CassandraTest.factories.getNodeFactory().create("test1", null, tagsMap).getId();

    long nodeVersionId1 = CassandraTest.factories.getNodeVersionFactory().create(tagsMap,
        -1, null, new HashMap<>(), nodeId, new ArrayList<>()).getId();
    long nodeVersionId2 = CassandraTest.factories.getNodeVersionFactory().create(tagsMap,
        -1, null, new HashMap<>(), nodeId, new ArrayList<>()).getId();

    List<Long> ids = CassandraTest.tagFactory.getVersionIdsByTag("testtag");

    super.cassandraClient.commit();

    assertTrue(ids.contains(nodeVersionId1));
    assertTrue(ids.contains(nodeVersionId2));
  }
}
