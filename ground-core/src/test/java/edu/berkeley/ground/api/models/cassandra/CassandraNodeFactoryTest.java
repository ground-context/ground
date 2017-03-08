package edu.berkeley.ground.api.models.cassandra;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.berkeley.ground.api.CassandraTest;
import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class CassandraNodeFactoryTest extends CassandraTest {

  public CassandraNodeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testNodeCreation() {
    try {
      Map<String, Tag> tagsMap = new HashMap<>();
      tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

      String testName = "test";
      CassandraNodeFactory nodeFactory = (CassandraNodeFactory) CassandraTest.factories.getNodeFactory();
      nodeFactory.create(testName, tagsMap);

      Node node = nodeFactory.retrieveFromDatabase(testName);
      assertEquals(testName, node.getName());
      assertEquals(tagsMap, node.getTags());
    } catch (GroundException ge) {
      fail(ge.getMessage());
    }
  }

  @Test
  public void testLeafRetrieval() throws GroundException {
    String nodeName = "testNode1";
    long nodeId = CassandraTest.factories.getNodeFactory().create(nodeName, new HashMap<>()).getId();

    long nodeVersionId = CassandraTest.factories.getNodeVersionFactory().create(new HashMap<>(),
        -1, null, new HashMap<>(), nodeId, new ArrayList<>()).getId();
    long secondNVId = CassandraTest.factories.getNodeVersionFactory().create(new HashMap<>(), -1,
        null, new HashMap<>(), nodeId, new ArrayList<>()).getId();

    List<Long> leaves = CassandraTest.factories.getNodeFactory().getLeaves(nodeName);

    assertTrue(leaves.contains(nodeVersionId));
    assertTrue(leaves.contains(secondNVId));
  }
}
