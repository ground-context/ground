package edu.berkeley.ground.api.models.cassandra;

import org.junit.Test;

import java.util.HashMap;
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
      CassandraNodeFactory nodeFactory = (CassandraNodeFactory) super.factories.getNodeFactory();
      nodeFactory.create(testName, tagsMap);

      Node node = nodeFactory.retrieveFromDatabase(testName);
      assertEquals(testName, node.getName());
      assertEquals(tagsMap, node.getTags());
    } catch (GroundException ge) {
      fail(ge.getMessage());
    }
  }
}
