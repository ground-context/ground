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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.ToDoubleBiFunction;

import javax.ws.rs.POST;

import edu.berkeley.ground.dao.PostgresTest;
import edu.berkeley.ground.exceptions.GroundItemNotFoundException;
import edu.berkeley.ground.model.models.Edge;
import edu.berkeley.ground.model.models.EdgeVersion;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.versions.VersionHistoryDag;
import edu.berkeley.ground.model.versions.VersionSuccessor;

import static org.junit.Assert.*;

public class PostgresEdgeFactoryTest extends PostgresTest {

  public PostgresEdgeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testEdgeCreation() throws GroundException {
    String testName = "test";
    String sourceKey = "testKey";

    String fromNodeName = "testNode1";
    String toNodeName = "testNode2";
    long fromNodeId = PostgresTest.createNode(fromNodeName).getId();
    long toNodeId = PostgresTest.createNode(toNodeName).getId();

    PostgresTest.edgesResource.createEdge(testName, fromNodeName, toNodeName, sourceKey,
        new HashMap<>());

    Edge edge = PostgresTest.edgesResource.getEdge(sourceKey);

    assertEquals(testName, edge.getName());
    assertEquals(fromNodeId, edge.getFromNodeId());
    assertEquals(toNodeId, edge.getToNodeId());
    assertEquals(sourceKey, edge.getSourceKey());
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadEdge() throws GroundException {
    String sourceKey = "test";

    try {
      PostgresTest.edgesResource.getEdge(sourceKey);
    } catch (GroundException e) {
      assertEquals(GroundItemNotFoundException.class, e.getClass());

      throw e;
    }
  }

  @Test(expected = GroundException.class)
  public void testCreateDuplicateEdge() throws GroundException {
    String edgeName = "edgeName";
    String edgeKey = "edgeKey";
    String fromNode = "fromNode";
    String toNode = "toNode";

    try {
      PostgresTest.createNode(fromNode);
      PostgresTest.createNode(toNode);

      PostgresTest.edgesResource.createEdge(edgeName, fromNode, toNode, edgeKey, new HashMap<>());
    } catch (GroundException e) {
      fail(e.getMessage());
    }

    PostgresTest.edgesResource.createEdge(edgeName, fromNode, toNode, edgeKey, new HashMap<>());
  }

  @Test
  public void testTruncation() throws GroundException {
    String firstTestNode = "firstTestNode";
    long firstTestNodeId = PostgresTest.createNode(firstTestNode).getId();
    long firstNodeVersionId = PostgresTest.createNodeVersion(firstTestNodeId).getId();

    String secondTestNode = "secondTestNode";
    long secondTestNodeId = PostgresTest.createNode(secondTestNode).getId();
    long secondNodeVersionId = PostgresTest.createNodeVersion(secondTestNodeId).getId();

    String edgeName = "testEdge";
    long edgeId = PostgresTest.createEdge(edgeName, firstTestNode, secondTestNode).getId();

    long edgeVersionId = PostgresTest.createEdgeVersion(edgeId, firstNodeVersionId,
        secondNodeVersionId).getId();

    // create new node versions in each of the nodes
    List<Long> parents = new ArrayList<>();
    parents.add(firstNodeVersionId);
    long newFirstNodeVersionId = PostgresTest.createNodeVersion(firstTestNodeId, parents).getId();

    parents.clear();
    parents.add(secondNodeVersionId);
    long newSecondNodeVersionId = PostgresTest.createNodeVersion(secondTestNodeId, parents)
        .getId();

    parents.clear();
    parents.add(edgeVersionId);
    long newEdgeVersionId = PostgresTest.createEdgeVersion(edgeId, newFirstNodeVersionId,
        newSecondNodeVersionId, parents).getId();

    PostgresTest.edgesResource.truncateEdge(edgeName, 1);

    VersionHistoryDag<?> dag = PostgresTest.versionHistoryDAGFactory.retrieveFromDatabase(edgeId);

    assertEquals(1, dag.getEdgeIds().size());
    VersionSuccessor<?> successor = PostgresTest.versionSuccessorFactory.retrieveFromDatabase(
        dag.getEdgeIds().get(0));

    PostgresTest.postgresClient.commit();
    assertEquals(0, successor.getFromId());
    assertEquals(newEdgeVersionId, successor.getToId());
  }
}
