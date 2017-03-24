package edu.berkeley.ground.dao.models.postgres;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.berkeley.ground.dao.PostgresTest;
import edu.berkeley.ground.model.models.Node;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class PostgresNodeFactoryTest extends PostgresTest {

  public PostgresNodeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testNodeCreation() throws GroundException {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

    String testName = "test";
    String sourceKey = "testKey";

    PostgresNodeFactory nodeFactory = (PostgresNodeFactory) super.factories.getNodeFactory();
    nodeFactory.create(testName, sourceKey, tagsMap);

    Node node = nodeFactory.retrieveFromDatabase(testName);

    assertEquals(testName, node.getName());
    assertEquals(tagsMap, node.getTags());
    assertEquals(sourceKey, node.getSourceKey());
  }

  @Test
  public void testLeafRetrieval() throws GroundException {
    String nodeName = "testNode1";
    long nodeId = super.factories.getNodeFactory().create(nodeName, null, new HashMap<>()).getId();

    long nodeVersionId = super.factories.getNodeVersionFactory().create(new HashMap<>(),
        -1, null, new HashMap<>(), nodeId, new ArrayList<>()).getId();
    long secondNVId = super.factories.getNodeVersionFactory().create(new HashMap<>(), -1,
        null, new HashMap<>(), nodeId, new ArrayList<>()).getId();

    List<Long> leaves = super.factories.getNodeFactory().getLeaves(nodeName);

    assertTrue(leaves.contains(nodeVersionId));
    assertTrue(leaves.contains(secondNVId));
  }
}
