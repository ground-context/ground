package edu.berkeley.ground.dao.models.cassandra;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.berkeley.ground.dao.CassandraTest;
import edu.berkeley.ground.model.models.Node;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class CassandraNodeFactoryTest extends CassandraTest {

  public CassandraNodeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testNodeCreation() throws GroundException {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

    String testName = "test";
    String sourceKey = "testKey";

    CassandraNodeFactory nodeFactory = (CassandraNodeFactory) CassandraTest.factories.getNodeFactory();
    nodeFactory.create(testName, sourceKey, tagsMap);

    Node node = nodeFactory.retrieveFromDatabase(testName);
    assertEquals(testName, node.getName());
    assertEquals(tagsMap, node.getTags());
    assertEquals(sourceKey, node.getSourceKey());
  }

  @Test
  public void testLeafRetrieval() throws GroundException {
    String nodeName = "testNode1";
    String sourceKey = "testKey";

    long nodeId = CassandraTest.factories.getNodeFactory().create(nodeName, sourceKey,
        new HashMap<>()).getId();

    long nodeVersionId = CassandraTest.factories.getNodeVersionFactory().create(new HashMap<>(),
        -1, null, new HashMap<>(), nodeId, new ArrayList<>()).getId();
    long secondNVId = CassandraTest.factories.getNodeVersionFactory().create(new HashMap<>(), -1,
        null, new HashMap<>(), nodeId, new ArrayList<>()).getId();

    List<Long> leaves = CassandraTest.factories.getNodeFactory().getLeaves(nodeName);

    assertTrue(leaves.contains(nodeVersionId));
    assertTrue(leaves.contains(secondNVId));
  }
}
