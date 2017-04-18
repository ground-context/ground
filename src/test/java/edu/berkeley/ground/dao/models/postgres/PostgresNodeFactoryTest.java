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

package edu.berkeley.ground.dao.models.postgres;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.berkeley.ground.dao.PostgresTest;
import edu.berkeley.ground.exceptions.GroundItemNotFoundException;
import edu.berkeley.ground.model.models.Node;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.versions.VersionHistoryDag;
import edu.berkeley.ground.model.versions.VersionSuccessor;

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

    PostgresTest.nodesResource.createNode(testName, sourceKey,tagsMap);

    Node node = PostgresTest.nodesResource.getNode(sourceKey);
    assertEquals(testName, node.getName());
    assertEquals(tagsMap, node.getTags());
    assertEquals(sourceKey, node.getSourceKey());
  }

  @Test
  public void testLeafRetrieval() throws GroundException {
    String sourceKey = "testNode1";
    long nodeId = PostgresTest.createNode(sourceKey).getId();

    long nodeVersionId = PostgresTest.createNodeVersion(nodeId).getId();
    long secondNodeVersionId = PostgresTest.createNodeVersion(nodeId).getId();

    List<Long> leaves = PostgresTest.nodesResource.getLatestVersions(sourceKey);

    assertTrue(leaves.contains(nodeVersionId));
    assertTrue(leaves.contains(secondNodeVersionId));
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadNode() throws GroundException {
    String sourceKey = "test";

    try {
      PostgresTest.nodesResource.getNode(sourceKey);
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
      PostgresTest.nodesResource.createNode(nodeName, nodeKey, new HashMap<>());
    } catch (GroundException e) {
      fail(e.getMessage());
    }

    PostgresTest.nodesResource.createNode(nodeName, nodeKey, new HashMap<>());
  }

  @Test
  public void testTruncation() throws GroundException {
    String testNode = "testNode";
    long testNodeId = PostgresTest.createNode(testNode).getId();
    long firstNodeVersionId = PostgresTest.createNodeVersion(testNodeId).getId();

    List<Long> parents = new ArrayList<>();
    parents.add(firstNodeVersionId);
    long newNodeVersionId = PostgresTest.createNodeVersion(testNodeId, parents).getId();

    PostgresTest.nodesResource.truncateNode(testNode, 1);

    VersionHistoryDag<?> dag = PostgresTest.versionHistoryDAGFactory
        .retrieveFromDatabase(testNodeId);
    assertEquals(1, dag.getEdgeIds().size());

    VersionSuccessor<?> successor = PostgresTest.versionSuccessorFactory.retrieveFromDatabase(
        dag.getEdgeIds().get(0));
    PostgresTest.postgresClient.commit();

    assertEquals(0, successor.getFromId());
    assertEquals(newNodeVersionId, successor.getToId());
  }

  @Test
  public void testBranchTruncation() throws GroundException {
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

    PostgresTest.nodesResource.truncateNode(testNode, 2);

    VersionHistoryDag<?> dag = PostgresTest.versionHistoryDAGFactory.retrieveFromDatabase(testNodeId);

    assertEquals(4, dag.getEdgeIds().size());

    Set<List<Long>> correctSuccessors = new HashSet<>();
    correctSuccessors.add(Arrays.asList(0L, firstParentId));
    correctSuccessors.add(Arrays.asList(0L, secondParentId));
    correctSuccessors.add(Arrays.asList(firstParentId, childId));
    correctSuccessors.add(Arrays.asList(secondParentId, childId));

    for (long successorId : dag.getEdgeIds()) {
      VersionSuccessor successor = PostgresTest.versionSuccessorFactory.retrieveFromDatabase(successorId);
      correctSuccessors.remove(Arrays.asList(successor.getFromId(), successor.getToId()));
    }

    assertTrue(correctSuccessors.isEmpty());

    PostgresTest.postgresClient.commit();
  }
}
