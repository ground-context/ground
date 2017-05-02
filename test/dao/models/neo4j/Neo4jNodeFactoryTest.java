/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dao.models.neo4j;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import dao.Neo4jTest;
import exceptions.GroundItemNotFoundException;
import models.models.Node;
import models.models.Tag;
import models.versions.GroundType;
import models.versions.Item;
import exceptions.GroundException;
import models.versions.VersionHistoryDag;
import models.versions.VersionSuccessor;

import static org.junit.Assert.*;

public class Neo4jNodeFactoryTest extends Neo4jTest {

  public Neo4jNodeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testNodeCreation() throws GroundException {
    try {
      Map<String, Tag> tagsMap = new HashMap<>();
      tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

      String testName = "test";
      String sourceKey = "testKey";

      Neo4jTest.nodeFactory.create(testName, sourceKey, tagsMap);
      Node node = Neo4jTest.nodeFactory.retrieveFromDatabase(sourceKey);

      assertEquals(testName, node.getName());
      assertEquals(tagsMap, node.getTags());
      assertEquals(sourceKey, node.getSourceKey());
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }

  @Test
  public void testNodeCreationWithoutTagValues() throws GroundException {
    try {
      Map<String, Tag> tagsMap = new HashMap<>();
      tagsMap.put("testtag", new Tag(1, "testtag", null, null));

      String testName = "test";
      String sourceKey = "testKey";

      Neo4jTest.nodeFactory.create(testName, sourceKey, tagsMap);
      Node node = Neo4jTest.nodeFactory.retrieveFromDatabase(sourceKey);

      assertEquals(testName, node.getName());
      assertEquals(tagsMap, node.getTags());
      assertEquals(sourceKey, node.getSourceKey());
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }

  @Test
  public void testNodeTagRetrieval() throws GroundException {
    try {
      Map<String, Tag> tags = Neo4jTest.createTags();
      String sourceKey = "testNode";

      long testNodeId = Neo4jTest.nodeFactory.create(null, sourceKey, tags).getId();
      Node retrieved = Neo4jTest.nodeFactory.retrieveFromDatabase(sourceKey);

      assertEquals(testNodeId, retrieved.getId());
      assertEquals(tags.size(), retrieved.getTags().size());

      Map<String, Tag> retrievedTags = retrieved.getTags();
      for (String key : tags.keySet()) {
        assert(retrievedTags).containsKey(key);
        assertEquals(tags.get(key), retrievedTags.get(key));
        assertEquals(retrieved.getId(), retrievedTags.get(key).getId());
      }
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }


  @Test
  public void testLeafRetrieval() throws GroundException {
    try {
      String sourceKey = "testNode1";
      long nodeId = Neo4jTest.createNode(sourceKey).getId();
      long nodeVersionId = Neo4jTest.createNodeVersion(nodeId).getId();
      long secondNodeVersionId = Neo4jTest.createNodeVersion(nodeId).getId();

      List<Long> leaves = Neo4jTest.nodeFactory.getLeaves(sourceKey);
      Neo4jTest.neo4jClient.commit();

      assertTrue(leaves.contains(nodeVersionId));
      assertTrue(leaves.contains(secondNodeVersionId));
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadNode() throws GroundException {
    String sourceKey = "test";

    try {
      Neo4jTest.nodeFactory.retrieveFromDatabase(sourceKey);
    } catch (GroundException e) {
      assertEquals(GroundItemNotFoundException.class, e.getClass());
      throw e;
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testCreateDuplicateNode() throws GroundException {
    String nodeName = "nodeName";
    String nodeKey = "nodeKey";

    try {
      try {
        Neo4jTest.nodeFactory.create(nodeName, nodeKey, new HashMap<>());
      } catch (GroundException e) {
        Neo4jTest.neo4jClient.abort();

        fail(e.getMessage());
      }

      Neo4jTest.nodeFactory.create(nodeName, nodeKey, new HashMap<>());
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }


  @Test
  public void testTruncation() throws GroundException {
    try {
      String testNode = "testNode";
      long nodeId = Neo4jTest.createNode(testNode).getId();
      long firstNodeVersionId = Neo4jTest.createNodeVersion(nodeId).getId();

      List<Long> parents = new ArrayList<>();
      parents.add(firstNodeVersionId);
      long newNodeVersionId = Neo4jTest.createNodeVersion(nodeId, parents).getId();

      Neo4jTest.nodeFactory.truncate(nodeId, 1);

      VersionHistoryDag<?> dag = Neo4jTest.versionHistoryDAGFactory.retrieveFromDatabase(nodeId);

      assertEquals(1, dag.getEdgeIds().size());

      VersionSuccessor<?> successor = Neo4jTest.versionSuccessorFactory.retrieveFromDatabase(
          dag.getEdgeIds().get(0));

      assertEquals(nodeId, successor.getFromId());
      assertEquals(newNodeVersionId, successor.getToId());
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }

  @Test
  public void testBranchTruncation() throws GroundException {
    try {
      String testNode = "testNode";
      long testNodeId = Neo4jTest.createNode(testNode).getId();
      long originalId = Neo4jTest.createNodeVersion(testNodeId).getId();

      List<Long> parents = new ArrayList<>();
      parents.add(originalId);
      long firstParentId = Neo4jTest.createNodeVersion(testNodeId, parents).getId();
      long secondParentId = Neo4jTest.createNodeVersion(testNodeId, parents).getId();

      parents.clear();
      parents.add(firstParentId);
      parents.add(secondParentId);
      long childId = Neo4jTest.createNodeVersion(testNodeId, parents).getId();

      Neo4jTest.nodeFactory.truncate(testNodeId, 2);

      VersionHistoryDag<?> dag = Neo4jTest.versionHistoryDAGFactory
          .retrieveFromDatabase(testNodeId);

      assertEquals(4, dag.getEdgeIds().size());

      Set<List<Long>> correctSuccessors = new HashSet<>();
      correctSuccessors.add(Arrays.asList(testNodeId, firstParentId));
      correctSuccessors.add(Arrays.asList(testNodeId, secondParentId));
      correctSuccessors.add(Arrays.asList(firstParentId, childId));
      correctSuccessors.add(Arrays.asList(secondParentId, childId));

      for (long successorId : dag.getEdgeIds()) {
        VersionSuccessor successor = Neo4jTest.versionSuccessorFactory
            .retrieveFromDatabase(successorId);
        correctSuccessors.remove(Arrays.asList(successor.getFromId(), successor.getToId()));
      }

      assertTrue(correctSuccessors.isEmpty());
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }
}
