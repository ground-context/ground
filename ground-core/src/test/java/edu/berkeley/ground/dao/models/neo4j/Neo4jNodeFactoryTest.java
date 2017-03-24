package edu.berkeley.ground.dao.models.neo4j;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.berkeley.ground.model.Neo4jTest;
import edu.berkeley.ground.model.models.Node;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.model.versions.Item;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class Neo4jNodeFactoryTest extends Neo4jTest {

  public Neo4jNodeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testNodeCreation() throws GroundException {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

    String testName = "test";
    Neo4jNodeFactory nodeFactory = (Neo4jNodeFactory) super.factories.getNodeFactory();
    nodeFactory.create(testName, tagsMap);

    Node node = nodeFactory.retrieveFromDatabase(testName);

    assertEquals(testName, node.getName());
    assertEquals(tagsMap, node.getTags());
  }

  @Test
  public void testNodeTagRetrieval() throws GroundException {
    try {
      Map<String, Tag> tags = new HashMap<>();
      tags.put("intfield", new Tag(-1, "intfield", 1, GroundType.INTEGER));
      tags.put("strfield", new Tag(-1, "strfield", "1", GroundType.STRING));
      tags.put("boolfield", new Tag(-1, "boolfield", true, GroundType.BOOLEAN));

      long testNodeId = super.factories.getNodeFactory().create("testNode", tags).getId();
      Item retrieved = super.itemFactory.retrieveFromDatabase(testNodeId);

      assertEquals(testNodeId, retrieved.getId());
      assertEquals(tags.size(), retrieved.getTags().size());

      Map<String, Tag> retrievedTags = retrieved.getTags();
      for (String key : tags.keySet()) {
        assert (retrievedTags).containsKey(key);
        assertEquals(tags.get(key), retrievedTags.get(key));
        assertEquals(retrieved.getId(), retrievedTags.get(key).getId());
      }
    } finally {
      super.neo4jClient.abort();
    }
  }


  @Test
  public void testLeafRetrieval() throws GroundException {
    String nodeName = "testNode1";
    long nodeId = super.factories.getNodeFactory().create(nodeName, new HashMap<>()).getId();

    long nodeVersionId = super.factories.getNodeVersionFactory().create(new HashMap<>(),
        -1, null, new HashMap<>(), nodeId, new ArrayList<>()).getId();
    long secondNVId = super.factories.getNodeVersionFactory().create(new HashMap<>(), -1,
        null, new HashMap<>(), nodeId, new ArrayList<>()).getId();

    List<Long> leaves = super.factories.getNodeFactory().getLeaves(nodeName);

    assertTrue(leaves.contains(nodeVersionId));
    assertTrue(leaves.contains(secondNVId));
  }
}
