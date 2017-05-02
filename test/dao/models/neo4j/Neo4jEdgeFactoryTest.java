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
import java.util.HashMap;
import java.util.List;

import dao.Neo4jTest;
import exceptions.GroundItemNotFoundException;
import models.models.Edge;
import exceptions.GroundException;
import models.versions.VersionHistoryDag;
import models.versions.VersionSuccessor;

import static org.junit.Assert.*;

public class Neo4jEdgeFactoryTest extends Neo4jTest {

  public Neo4jEdgeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testEdgeCreation() throws GroundException {
    try {
      String testName = "test";
      String sourceKey = "testKey";

      String firstNodeName = "firstNode";
      long firstNodeId = Neo4jTest.createNode(firstNodeName).getId();
      String secondNodeName = "secondNode";
      long secondNodeId = Neo4jTest.createNode(secondNodeName).getId();

      Neo4jTest.edgeFactory.create(testName, sourceKey, firstNodeId, secondNodeId,
          new HashMap<>());

      Edge edge = Neo4jTest.edgeFactory.retrieveFromDatabase(sourceKey);

      assertEquals(testName, edge.getName());
      assertEquals(firstNodeId, edge.getFromNodeId());
      assertEquals(secondNodeId, edge.getToNodeId());
      assertEquals(sourceKey, edge.getSourceKey());
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadEdge() throws GroundException {
    String sourceKey = "test";

    try {
      Neo4jTest.edgeFactory.retrieveFromDatabase(sourceKey);
    } catch (GroundException e) {
      assertEquals(GroundItemNotFoundException.class, e.getClass());
      throw e;
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testCreateDuplicateEdge() throws GroundException {
    String edgeName = "edgeName";
    String edgeKey = "edgeKey";
    String fromNode = "fromNode";
    String toNode = "toNode";

    long fromNodeId = -1;
    long toNodeId = -1;

    try {
      try {
        fromNodeId = Neo4jTest.createNode(fromNode).getId();
        toNodeId = Neo4jTest.createNode(toNode).getId();

        Neo4jTest.edgeFactory.create(edgeName, edgeKey, fromNodeId, toNodeId, new HashMap<>());
      } catch (GroundException e) {
        Neo4jTest.neo4jClient.abort();

        fail(e.getMessage());
      }

      Neo4jTest.edgeFactory.create(edgeName, edgeKey, fromNodeId, toNodeId, new HashMap<>());
    } finally {
      Neo4jTest.neo4jClient.abort();
    }

    Neo4jTest.neo4jClient.commit();
  }


  @Test
  public void testTruncation() throws GroundException {
    try {
      String firstTestNode = "firstTestNode";
      long firstTestNodeId = Neo4jTest.createNode(firstTestNode).getId();
      long firstNodeVersionId = Neo4jTest.createNodeVersion(firstTestNodeId).getId();

      String secondTestNode = "secondTestNode";
      long secondTestNodeId = Neo4jTest.createNode(secondTestNode).getId();
      long secondNodeVersionId = Neo4jTest.createNodeVersion(secondTestNodeId).getId();

      String edgeName = "testEdge";
      long edgeId = Neo4jTest.createEdge(edgeName, firstTestNode, secondTestNode).getId();

      long edgeVersionId = Neo4jTest.createEdgeVersion(edgeId, firstNodeVersionId,
          secondNodeVersionId).getId();

      // create new node versions in each of the nodes
      List<Long> parents = new ArrayList<>();
      parents.add(firstNodeVersionId);
      long newFirstNodeVersionId = Neo4jTest.createNodeVersion(firstTestNodeId, parents).getId();

      parents.clear();
      parents.add(secondNodeVersionId);
      long newSecondNodeVersionId = Neo4jTest.createNodeVersion(secondTestNodeId, parents).getId();

      parents.clear();
      parents.add(edgeVersionId);
      long newEdgeVersionId = Neo4jTest.createEdgeVersion(edgeId, newFirstNodeVersionId,
          newSecondNodeVersionId, parents).getId();

      Neo4jTest.edgeFactory.truncate(edgeId, 1);

      VersionHistoryDag<?> dag = Neo4jTest.versionHistoryDAGFactory.retrieveFromDatabase(edgeId);

      assertEquals(1, dag.getEdgeIds().size());

      VersionSuccessor<?> successor = Neo4jTest.versionSuccessorFactory.retrieveFromDatabase(
          dag.getEdgeIds().get(0));

      assertEquals(edgeId, successor.getFromId());
      assertEquals(newEdgeVersionId, successor.getToId());
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }
}
