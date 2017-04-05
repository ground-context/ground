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

package edu.berkeley.ground.dao.models.cassandra;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.berkeley.ground.dao.CassandraTest;
import edu.berkeley.ground.model.models.Node;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.versions.VersionHistoryDag;
import edu.berkeley.ground.model.versions.VersionSuccessor;

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

  @Test(expected = GroundException.class)
  public void testRetrieveBadNode() throws GroundException {
    String testName = "test";

    try {
      CassandraTest.factories.getNodeFactory().retrieveFromDatabase(testName);
    } catch (GroundException e) {
      assertEquals("No Node found with name " + testName + ".", e.getMessage());

      throw e;
    }
  }

  @Test
  public void testTruncation() throws GroundException {
    String testNode = "testNode";
    long testNodeId = CassandraTest.factories.getNodeFactory().create(testNode, null,
        new HashMap<>()).getId();
    long firstNodeVersionId = CassandraTest.factories.getNodeVersionFactory().create(new HashMap<>(),
        -1, null, new HashMap<>(), testNodeId, new ArrayList<>()).getId();

    List<Long> parents = new ArrayList<>();
    parents.add(firstNodeVersionId);
    long newNodeVersionId = CassandraTest.factories.getNodeVersionFactory().create(new
        HashMap<>(), -1, null, new HashMap<>(), testNodeId, parents).getId();

    CassandraTest.factories.getNodeFactory().truncate(testNodeId, 1);

    VersionHistoryDag<?> dag = CassandraTest.versionHistoryDAGFactory.retrieveFromDatabase(testNodeId);

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
    long testNodeId = CassandraTest.factories.getNodeFactory().create(testNode, null,
        new HashMap<>()).getId();
    long originalId = CassandraTest.factories.getNodeVersionFactory().create(new HashMap<>(),
        -1, null, new HashMap<>(), testNodeId, new ArrayList<>()).getId();

    List<Long> parents = new ArrayList<>();
    parents.add(originalId);
    long firstParentId = CassandraTest.factories.getNodeVersionFactory().create(new
        HashMap<>(), -1, null, new HashMap<>(), testNodeId, parents).getId();

    long secondParentId = CassandraTest.factories.getNodeVersionFactory().create(new
        HashMap<>(), -1, null, new HashMap<>(), testNodeId, parents).getId();

    parents.clear();
    parents.add(firstParentId);
    parents.add(secondParentId);

    long childId = CassandraTest.factories.getNodeVersionFactory().create(new
        HashMap<>(), -1, null, new HashMap<>(), testNodeId, parents).getId();

    CassandraTest.factories.getNodeFactory().truncate(testNodeId, 2);

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
