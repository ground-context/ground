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
import java.util.HashMap;
import java.util.List;

import edu.berkeley.ground.dao.CassandraTest;
import edu.berkeley.ground.exceptions.GroundItemNotFoundException;
import edu.berkeley.ground.model.models.Edge;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.versions.VersionHistoryDag;
import edu.berkeley.ground.model.versions.VersionSuccessor;

import static org.junit.Assert.*;

public class CassandraEdgeFactoryTest extends CassandraTest {

  public CassandraEdgeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testEdgeCreation() throws GroundException {
    String testName = "test";
    String sourceKey = "testKey";

    String firstTestNode = "firstTestNode";
    long firstTestNodeId = CassandraTest.createNode(firstTestNode).getId();

    String secondTestNode = "secondTestNode";
    long secondTestNodeId = CassandraTest.createNode(secondTestNode).getId();

    CassandraTest.edgesResource.createEdge(testName, firstTestNode, secondTestNode, sourceKey,
        new HashMap<>());

    Edge edge = CassandraTest.edgesResource.getEdge(sourceKey);

    assertEquals(testName, edge.getName());
    assertEquals(firstTestNodeId, edge.getFromNodeId());
    assertEquals(secondTestNodeId, edge.getToNodeId());
    assertEquals(sourceKey, edge.getSourceKey());
  }

  @Test(expected = GroundException.class)
  public void testCreateDuplicateEdge() throws GroundException {
    String edgeName = "edgeName";
    String edgeKey = "edgeKey";
    String fromNode = "fromNode";
    String toNode = "toNode";

    try {
      CassandraTest.createNode(fromNode);
      CassandraTest.createNode(toNode);

      CassandraTest.edgesResource.createEdge(edgeName, fromNode, toNode, edgeKey, new HashMap<>());
    } catch (GroundException e) {
      fail(e.getMessage());
    }

    CassandraTest.edgesResource.createEdge(edgeName, fromNode, toNode, edgeKey, new HashMap<>());
  }


  @Test(expected = GroundException.class)
  public void testRetrieveBadEdge() throws GroundException {
    String sourceKey = "test";

    try {
      CassandraTest.edgesResource.getEdge(sourceKey);
    } catch (GroundException e) {
      assertEquals(GroundItemNotFoundException.class, e.getClass());

      throw e;
    }
  }

  @Test
  public void testTruncation() throws GroundException {
    String firstTestNode = "firstTestNode";
    long firstTestNodeId = CassandraTest.createNode(firstTestNode).getId();
    long firstNodeVersionId = CassandraTest.createNodeVersion(firstTestNodeId).getId();

    String secondTestNode = "secondTestNode";
    long secondTestNodeId = CassandraTest.createNode(secondTestNode).getId();
    long secondNodeVersionId = CassandraTest.createNodeVersion(secondTestNodeId).getId();

    String edgeName = "testEdge";
    long edgeId = CassandraTest.createEdge(edgeName, firstTestNode, secondTestNode).getId();

    long edgeVersionId = CassandraTest.createEdgeVersion(edgeId, firstNodeVersionId,
        secondNodeVersionId).getId();

    // create new node versions in each of the nodes
    List<Long> parents = new ArrayList<>();
    parents.add(firstNodeVersionId);
    long newFirstNodeVersionId = CassandraTest.createNodeVersion(firstTestNodeId, parents).getId();

    parents.clear();
    parents.add(secondNodeVersionId);
    long newSecondNodeVersionId = CassandraTest.createNodeVersion(secondTestNodeId, parents)
        .getId();

    parents.clear();
    parents.add(edgeVersionId);
    long newEdgeVersionId = CassandraTest.createEdgeVersion(edgeId, newFirstNodeVersionId,
        newSecondNodeVersionId, parents).getId();

    CassandraTest.edgesResource.truncateEdge(edgeName, 1);

    VersionHistoryDag<?> dag = CassandraTest.versionHistoryDAGFactory.retrieveFromDatabase(edgeId);

    assertEquals(1, dag.getEdgeIds().size());
    VersionSuccessor<?> successor = CassandraTest.versionSuccessorFactory.retrieveFromDatabase(
        dag.getEdgeIds().get(0));

    CassandraTest.cassandraClient.commit();
    assertEquals(0, successor.getFromId());
    assertEquals(newEdgeVersionId, successor.getToId());
  }
}
