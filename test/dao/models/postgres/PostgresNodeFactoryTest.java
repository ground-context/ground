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

package dao.models.postgres;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import dao.PostgresTest;
import exceptions.GroundItemNotFoundException;
import models.models.Node;
import models.models.Tag;
import models.versions.GroundType;
import exceptions.GroundException;
import models.versions.VersionHistoryDag;
import models.versions.VersionSuccessor;

import static org.junit.Assert.*;

public class PostgresNodeFactoryTest extends PostgresTest {

  public PostgresNodeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testNodeCreation() throws GroundException {
    try {
      Map<String, Tag> tagsMap = new HashMap<>();
      tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

      String testName = "test";
      String sourceKey = "testKey";

      PostgresTest.nodeFactory.create(testName, sourceKey, tagsMap);

      Node node = PostgresTest.nodeFactory.retrieveFromDatabase(sourceKey);
      assertEquals(testName, node.getName());
      assertEquals(tagsMap, node.getTags());
      assertEquals(sourceKey, node.getSourceKey());
    } finally {
      PostgresTest.postgresClient.commit();
    }
  }

  @Test
  public void testLeafRetrieval() throws GroundException {
    try {
      String sourceKey = "testNode1";
      long nodeId = PostgresTest.createNode(sourceKey).getId();

      long nodeVersionId = PostgresTest.createNodeVersion(nodeId).getId();
      long secondNodeVersionId = PostgresTest.createNodeVersion(nodeId).getId();

      List<Long> leaves = PostgresTest.nodeFactory.getLeaves(sourceKey);

      assertTrue(leaves.contains(nodeVersionId));
      assertTrue(leaves.contains(secondNodeVersionId));
    } finally {
      PostgresTest.postgresClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadNode() throws GroundException {
    String sourceKey = "test";

    try {
      PostgresTest.nodeFactory.retrieveFromDatabase(sourceKey);
    } catch (GroundException e) {
      assertEquals(GroundItemNotFoundException.class, e.getClass());

      throw e;
    } finally {
      PostgresTest.postgresClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testCreateDuplicateNode() throws GroundException {
    String nodeName = "nodeName";
    String nodeKey = "nodeKey";

    try {
      try {
        PostgresTest.nodeFactory.create(nodeName, nodeKey, new HashMap<>());
      } catch (GroundException e) {
        fail(e.getMessage());
      }

      PostgresTest.nodeFactory.create(nodeName, nodeKey, new HashMap<>());
    } finally {
      PostgresTest.postgresClient.commit();
    }
  }

  @Test
  public void testTruncation() throws GroundException {
    try {
      String testNode = "testNode";
      long testNodeId = PostgresTest.createNode(testNode).getId();
      long firstNodeVersionId = PostgresTest.createNodeVersion(testNodeId).getId();

      List<Long> parents = new ArrayList<>();
      parents.add(firstNodeVersionId);
      long newNodeVersionId = PostgresTest.createNodeVersion(testNodeId, parents).getId();

      PostgresTest.nodeFactory.truncate(testNodeId, 1);

      VersionHistoryDag<?> dag = PostgresTest.versionHistoryDAGFactory
          .retrieveFromDatabase(testNodeId);
      assertEquals(1, dag.getEdgeIds().size());

      VersionSuccessor<?> successor = PostgresTest.versionSuccessorFactory.retrieveFromDatabase(
          dag.getEdgeIds().get(0));
      PostgresTest.postgresClient.commit();

      assertEquals(0, successor.getFromId());
      assertEquals(newNodeVersionId, successor.getToId());
    } finally {
      PostgresTest.postgresClient.commit();
    }
  }

  @Test
  public void testBranchTruncation() throws GroundException {
    try {
      String testNode = "testNode";
      long testNodeId = PostgresTest.createNode(testNode).getId();
      long originalId = PostgresTest.createNodeVersion(testNodeId).getId();

      List<Long> parents = new ArrayList<>();
      parents.add(originalId);
      long firstParentId = PostgresTest.createNodeVersion(testNodeId, parents).getId();
      long secondParentId = PostgresTest.createNodeVersion(testNodeId, parents).getId();

      parents.clear();
      parents.add(firstParentId);
      parents.add(secondParentId);
      long childId = PostgresTest.createNodeVersion(testNodeId, parents).getId();

      PostgresTest.nodeFactory.truncate(testNodeId, 2);

      VersionHistoryDag<?> dag = PostgresTest.versionHistoryDAGFactory
          .retrieveFromDatabase(testNodeId);

      assertEquals(4, dag.getEdgeIds().size());

      Set<List<Long>> correctSuccessors = new HashSet<>();
      correctSuccessors.add(Arrays.asList(0L, firstParentId));
      correctSuccessors.add(Arrays.asList(0L, secondParentId));
      correctSuccessors.add(Arrays.asList(firstParentId, childId));
      correctSuccessors.add(Arrays.asList(secondParentId, childId));

      for (long successorId : dag.getEdgeIds()) {
        VersionSuccessor successor = PostgresTest.versionSuccessorFactory
            .retrieveFromDatabase(successorId);
        correctSuccessors.remove(Arrays.asList(successor.getFromId(), successor.getToId()));
      }

      assertTrue(correctSuccessors.isEmpty());
    } finally {
      PostgresTest.postgresClient.commit();
    }
  }
}
