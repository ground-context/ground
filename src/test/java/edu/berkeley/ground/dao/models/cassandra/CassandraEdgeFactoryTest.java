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

import com.sun.org.apache.bcel.internal.generic.CASTORE;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.berkeley.ground.dao.CassandraTest;
import edu.berkeley.ground.model.models.Edge;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.EdgeVersion;
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

    long fromNodeId = 1;
    long toNodeId = 2;

    CassandraEdgeFactory edgeFactory = (CassandraEdgeFactory) CassandraTest.factories
        .getEdgeFactory();
    edgeFactory.create(testName, sourceKey, fromNodeId, toNodeId, new HashMap<>());

    Edge edge = edgeFactory.retrieveFromDatabase(testName);

    assertEquals(testName, edge.getName());
    assertEquals(fromNodeId, edge.getFromNodeId());
    assertEquals(toNodeId, edge.getToNodeId());
    assertEquals(sourceKey, edge.getSourceKey());
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadEdge() throws GroundException {
    String testName = "test";

    try {
      CassandraTest.factories.getEdgeFactory().retrieveFromDatabase(testName);
    } catch (GroundException e) {
      assertEquals("No Edge found with name " + testName + ".", e.getMessage());

      throw e;
    }
  }

  @Test
  public void testTruncation() throws GroundException {
    String firstTestNode = "firstTestNode";
    long firstTestNodeId = CassandraTest.factories.getNodeFactory().create(firstTestNode, null,
        new HashMap<>()).getId();
    long firstNodeVersionId = CassandraTest.factories.getNodeVersionFactory().create(new HashMap<>(),
        -1, null, new HashMap<>(), firstTestNodeId, new ArrayList<>()).getId();

    String secondTestNode = "secondTestNode";
    long secondTestNodeId = CassandraTest.factories.getNodeFactory().create(secondTestNode, null,
        new HashMap<>()).getId();
    long secondNodeVersionId = CassandraTest.factories.getNodeVersionFactory().create(new HashMap<>(),
        -1, null, new HashMap<>(), secondTestNodeId, new ArrayList<>()).getId();

    String edgeName = "testEdge";
    long edgeId = CassandraTest.factories.getEdgeFactory().create(edgeName, null, firstTestNodeId,
        secondTestNodeId, new HashMap<>()).getId();

    long edgeVersionId = CassandraTest.factories.getEdgeVersionFactory().create(new HashMap<>(),
        -1, null, new HashMap<>(), edgeId, firstNodeVersionId, -1, secondNodeVersionId, -1,
        new ArrayList<>()).getId();

    // create new node versions in each of the nodes
    List<Long> parents = new ArrayList<>();
    parents.add(firstNodeVersionId);
    long newFirstNodeVersionId = CassandraTest.factories.getNodeVersionFactory().create(new
        HashMap<>(), -1, null, new HashMap<>(), firstTestNodeId, parents).getId();

    parents.clear();
    parents.add(secondNodeVersionId);
    long newSecondNodeVersionId = CassandraTest.factories.getNodeVersionFactory().create(new
        HashMap<>(), -1, null, new HashMap<>(), secondTestNodeId, parents).getId();

    parents.clear();
    parents.add(edgeVersionId);
    long newEdgeVersionId = CassandraTest.factories.getEdgeVersionFactory().create(new HashMap<>(),
        -1, null, new HashMap<>(), edgeId, newFirstNodeVersionId, -1, newSecondNodeVersionId, -1,
        parents).getId();

    CassandraTest.factories.getEdgeFactory().truncate(edgeId, 1);

    VersionHistoryDag<?> dag = CassandraTest.versionHistoryDAGFactory.retrieveFromDatabase(edgeId);

    assertEquals(1, dag.getEdgeIds().size());

    VersionSuccessor<?> successor = CassandraTest.versionSuccessorFactory.retrieveFromDatabase(
        dag.getEdgeIds().get(0));

    CassandraTest.cassandraClient.commit();

    assertEquals(0, successor.getFromId());
    assertEquals(newEdgeVersionId, successor.getToId());
  }
}
