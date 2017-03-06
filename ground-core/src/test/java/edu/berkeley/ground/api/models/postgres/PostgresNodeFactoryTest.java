package edu.berkeley.ground.api.models.postgres;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import edu.berkeley.ground.api.PostgresTest;
import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
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
    PostgresNodeFactory nodeFactory = (PostgresNodeFactory) super.factories.getNodeFactory();
    nodeFactory.create(testName, tagsMap);

    Node node = nodeFactory.retrieveFromDatabase(testName);

    assertEquals(testName, node.getName());
    assertEquals(tagsMap, node.getTags());
  }
}
