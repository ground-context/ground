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

import dao.Neo4jTest;
import db.PostgresClient;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import dao.PostgresTest;
import exceptions.GroundItemNotFoundException;
import models.models.Edge;
import exceptions.GroundException;
import models.versions.VersionHistoryDag;
import models.versions.VersionSuccessor;

import static org.junit.Assert.*;

public class PostgresEdgeFactoryTest extends PostgresTest {

  public PostgresEdgeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testEdgeCreation() throws GroundException {
    try {
      String testName = "test";
      String sourceKey = "testKey";

      String fromNodeName = "testNode1";
      String toNodeName = "testNode2";
      long fromNodeId = PostgresTest.createNode(fromNodeName).getId();
      long toNodeId = PostgresTest.createNode(toNodeName).getId();

      PostgresTest.edgeFactory.create(testName, sourceKey, fromNodeId, toNodeId, new HashMap<>());

      Edge edge = PostgresTest.edgeFactory.retrieveFromDatabase(sourceKey);

      assertEquals(testName, edge.getName());
      assertEquals(fromNodeId, edge.getFromNodeId());
      assertEquals(toNodeId, edge.getToNodeId());
      assertEquals(sourceKey, edge.getSourceKey());
    } finally {
      PostgresTest.postgresClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadEdge() throws GroundException {
    String sourceKey = "test";

    try {
      PostgresTest.edgeFactory.retrieveFromDatabase(sourceKey);
    } catch (GroundException e) {
      assertEquals(GroundItemNotFoundException.class, e.getClass());

      throw e;
    } finally {
      PostgresTest.postgresClient.commit();
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
        fromNodeId = PostgresTest.createNode(fromNode).getId();
        toNodeId = PostgresTest.createNode(toNode).getId();

        PostgresTest.edgeFactory.create(edgeName, edgeKey, fromNodeId, toNodeId, new HashMap<>());
      } catch (GroundException e) {
        fail(e.getMessage());
      }

      PostgresTest.edgeFactory.create(edgeName, edgeKey, fromNodeId, toNodeId, new HashMap<>());
    } finally {
      PostgresTest.postgresClient.commit();
    }
  }

  @Test
  public void testTruncation() throws GroundException {
    try {
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

      PostgresTest.edgeFactory.truncate(edgeId, 1);

      VersionHistoryDag<?> dag = PostgresTest.versionHistoryDAGFactory.retrieveFromDatabase(edgeId);

      assertEquals(1, dag.getEdgeIds().size());
      VersionSuccessor<?> successor = PostgresTest.versionSuccessorFactory.retrieveFromDatabase(
          dag.getEdgeIds().get(0));

      PostgresTest.postgresClient.commit();
      assertEquals(0, successor.getFromId());
      assertEquals(newEdgeVersionId, successor.getToId());
    } finally {
      PostgresTest.postgresClient.commit();
    }
  }
}
