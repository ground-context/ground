package edu.berkeley.ground.postgres.dao.core;

/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.core.Edge;
import edu.berkeley.ground.common.model.version.VersionHistoryDag;
import edu.berkeley.ground.common.model.version.VersionSuccessor;
import edu.berkeley.ground.postgres.dao.PostgresTest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.junit.Test;

public class PostgresEdgeDaoTest extends PostgresTest {

  public PostgresEdgeDaoTest() throws GroundException {
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

    PostgresTest.edgeDao.create(new Edge(0L, testName, sourceKey, fromNodeId, toNodeId, new HashMap<>()));

    Edge edge = PostgresTest.edgeDao.retrieveFromDatabase(sourceKey);

    assertEquals(testName, edge.getName());
    assertEquals(fromNodeId, edge.getFromNodeId());
    assertEquals(toNodeId, edge.getToNodeId());
    assertEquals(sourceKey, edge.getSourceKey());
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadEdge() throws GroundException {
    String sourceKey = "test";

    try {
      PostgresTest.edgeDao.retrieveFromDatabase(sourceKey);
    } catch (GroundException e) {
      assertEquals(GroundException.class, e.getClass());

      throw e;
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
      fromNodeId = PostgresTest.createNode(fromNode).getId();
      toNodeId = PostgresTest.createNode(toNode).getId();

      PostgresTest.edgeDao
        .create(new Edge(0L, edgeName, edgeKey, fromNodeId, toNodeId, new HashMap<>()));
    } catch (GroundException e) {
      fail(e.getMessage());
    }

    PostgresTest.edgeDao
      .create(new Edge(0L, edgeName, edgeKey, fromNodeId, toNodeId, new HashMap<>()));
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

    long edgeVersionId = PostgresTest.createEdgeVersion(edgeId, firstNodeVersionId, secondNodeVersionId).getId();

    // create new node versions in each of the nodes
    List<Long> parents = new ArrayList<>();
    parents.add(firstNodeVersionId);
    long newFirstNodeVersionId = PostgresTest.createNodeVersion(firstTestNodeId, parents).getId();

    parents.clear();
    parents.add(secondNodeVersionId);
    long newSecondNodeVersionId = PostgresTest.createNodeVersion(secondTestNodeId, parents).getId();

    parents.clear();
    parents.add(edgeVersionId);
    long newEdgeVersionId = PostgresTest.createEdgeVersion(edgeId, newFirstNodeVersionId, newSecondNodeVersionId, parents).getId();

    PostgresTest.edgeDao.truncate(edgeId, 1);

    VersionHistoryDag dag = PostgresTest.versionHistoryDagDao.retrieveFromDatabase(edgeId);

    assertEquals(1, dag.getEdgeIds().size());
    VersionSuccessor successor = PostgresTest.versionSuccessorDao.retrieveFromDatabase(dag.getEdgeIds().get(0));

    assertEquals(0, successor.getFromId());
    assertEquals(newEdgeVersionId, successor.getToId());
  }
}
