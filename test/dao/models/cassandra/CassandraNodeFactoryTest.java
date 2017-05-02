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

package dao.models.cassandra;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import dao.CassandraTest;
import exceptions.GroundItemNotFoundException;
import models.models.Node;
import models.models.Tag;
import models.versions.GroundType;
import exceptions.GroundException;
import models.versions.VersionHistoryDag;
import models.versions.VersionSuccessor;

import static org.junit.Assert.*;

public class CassandraNodeFactoryTest extends CassandraTest {

  public CassandraNodeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testNodeCreation() throws GroundException {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag", new Tag(1, -1, "testtag", "tag", GroundType.STRING));

    String testName = "test";
    String sourceKey = "testKey";

    CassandraTest.nodeFactory.create(testName, sourceKey,tagsMap);
    Node node = CassandraTest.nodeFactory.retrieveFromDatabase(sourceKey);

    assertEquals(testName, node.getName());
    assertEquals(tagsMap, node.getTags());
    assertEquals(sourceKey, node.getSourceKey());
  }

  @Test
  public void testLeafRetrieval() throws GroundException {
    String sourceKey = "testNode1";
    long nodeId = CassandraTest.createNode(sourceKey).getId();

    long nodeVersionId = CassandraTest.createNodeVersion(nodeId).getId();
    long secondNodeVersionId = CassandraTest.createNodeVersion(nodeId).getId();

    List<Long> leaves = CassandraTest.nodeFactory.getLeaves(sourceKey);

    assertTrue(leaves.contains(nodeVersionId));
    assertTrue(leaves.contains(secondNodeVersionId));
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadNode() throws GroundException {
    String sourceKey = "test";

    try {
      CassandraTest.nodeFactory.retrieveFromDatabase(sourceKey);
    } catch (GroundException e) {
      assertEquals(GroundItemNotFoundException.class, e.getClass());

      throw e;
    }
  }

  @Test(expected = GroundException.class)
  public void testCreateDuplicateNode() throws GroundException {
    String nodeName = "nodeName";
    String nodeKey = "nodeKey";

    try {
      CassandraTest.nodeFactory.create(nodeName, nodeKey, new HashMap<>());
    } catch (GroundException e) {
      fail(e.getMessage());
    }

    CassandraTest.nodeFactory.create(nodeName, nodeKey, new HashMap<>());
  }

  @Test
  public void testTruncation() throws GroundException {
    String testNode = "testNode";
    long testNodeId = CassandraTest.createNode(testNode).getId();
    long firstNodeVersionId = CassandraTest.createNodeVersion(testNodeId).getId();

    List<Long> parents = new ArrayList<>();
    parents.add(firstNodeVersionId);
    long newNodeVersionId = CassandraTest.createNodeVersion(testNodeId, parents).getId();

    CassandraTest.nodeFactory.truncate(testNodeId, 1);

    VersionHistoryDag<?> dag = CassandraTest.versionHistoryDAGFactory
        .retrieveFromDatabase(testNodeId);
    assertEquals(1, dag.getEdgeIds().size());

    VersionSuccessor<?> successor = CassandraTest.versionSuccessorFactory.retrieveFromDatabase(
        dag.getEdgeIds().get(0));
    CassandraTest.cassandraClient.commit();

    assertEquals(0, successor.getFromId());
    assertEquals(newNodeVersionId, successor.getToId());
  }

  @Test
  public void testBranchTruncation() throws GroundException {
    String testNode = "testNode";
    long testNodeId = CassandraTest.createNode(testNode).getId();
    long originalId = CassandraTest.createNodeVersion(testNodeId).getId();

    List<Long> parents = new ArrayList<>();
    parents.add(originalId);
    long firstParentId = CassandraTest.createNodeVersion(testNodeId, parents).getId();
    long secondParentId = CassandraTest.createNodeVersion(testNodeId, parents).getId();

    parents.clear();
    parents.add(firstParentId);
    parents.add(secondParentId);
    long childId = CassandraTest.createNodeVersion(testNodeId, parents).getId();

    CassandraTest.nodeFactory.truncate(testNodeId, 2);

    VersionHistoryDag<?> dag = CassandraTest.versionHistoryDAGFactory.retrieveFromDatabase(testNodeId);

    assertEquals(4, dag.getEdgeIds().size());

    Set<List<Long>> correctSuccessors = new HashSet<>();
    correctSuccessors.add(Arrays.asList(0L, firstParentId));
    correctSuccessors.add(Arrays.asList(0L, secondParentId));
    correctSuccessors.add(Arrays.asList(firstParentId, childId));
    correctSuccessors.add(Arrays.asList(secondParentId, childId));

    for (long successorId : dag.getEdgeIds()) {
      VersionSuccessor successor = CassandraTest.versionSuccessorFactory.retrieveFromDatabase(successorId);
      correctSuccessors.remove(Arrays.asList(successor.getFromId(), successor.getToId()));
    }

    assertTrue(correctSuccessors.isEmpty());

    CassandraTest.cassandraClient.commit();
  }
}
