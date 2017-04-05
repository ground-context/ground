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

import edu.berkeley.ground.dao.PostgresTest;
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

    Map<String, Tag> tagsMap = new HashMap<>();

    PostgresNodeFactory nodeFactory = (PostgresNodeFactory) super.factories.getNodeFactory();
    long fromNodeId = nodeFactory.create("testNode1", null, tagsMap).getId();
    long toNodeId = nodeFactory.create("testNode2", null, tagsMap).getId();

    PostgresEdgeFactory edgeFactory = (PostgresEdgeFactory) super.factories.getEdgeFactory();
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
      super.factories.getEdgeFactory().retrieveFromDatabase(testName);
    } catch (GroundException e) {
      assertEquals("No Edge found with name " + testName + ".", e.getMessage());

      throw e;
    }
  }

  @Test
  public void testTruncation() throws GroundException {
    String firstTestNode = "firstTestNode";
    long firstTestNodeId = super.factories.getNodeFactory().create(firstTestNode, null,
        new HashMap<>()).getId();
    long firstNodeVersionId = super.factories.getNodeVersionFactory().create(new HashMap<>(),
        -1, null, new HashMap<>(), firstTestNodeId, new ArrayList<>()).getId();

    String secondTestNode = "secondTestNode";
    long secondTestNodeId = super.factories.getNodeFactory().create(secondTestNode, null,
        new HashMap<>()).getId();
    long secondNodeVersionId = super.factories.getNodeVersionFactory().create(new HashMap<>(),
        -1, null, new HashMap<>(), secondTestNodeId, new ArrayList<>()).getId();

    String edgeName = "testEdge";
    long edgeId = super.factories.getEdgeFactory().create(edgeName, null, firstTestNodeId,
        secondTestNodeId, new HashMap<>()).getId();


    long edgeVersionId = super.factories.getEdgeVersionFactory().create(new HashMap<>(),
        -1, null, new HashMap<>(), edgeId, firstNodeVersionId, -1, secondNodeVersionId, -1,
        new ArrayList<>()).getId();

    // create new node versions in each of the nodes
    List<Long> parents = new ArrayList<>();
    parents.add(firstNodeVersionId);
    long newFirstNodeVersionId = super.factories.getNodeVersionFactory().create(new
        HashMap<>(), -1, null, new HashMap<>(), firstTestNodeId, parents).getId();

    parents.clear();
    parents.add(secondNodeVersionId);
    long newSecondNodeVersionId = super.factories.getNodeVersionFactory().create(new
        HashMap<>(), -1, null, new HashMap<>(), secondTestNodeId, parents).getId();

    parents.clear();
    parents.add(edgeVersionId);
    long newEdgeVersionId = super.factories.getEdgeVersionFactory().create(new HashMap<>(),
        -1, null, new HashMap<>(), edgeId, newFirstNodeVersionId, -1, newSecondNodeVersionId, -1,
        parents).getId();

    super.factories.getEdgeFactory().truncate(edgeId, 1);

    VersionHistoryDag<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(edgeId);

    assertEquals(1, dag.getEdgeIds().size());

    VersionSuccessor<?> successor = super.versionSuccessorFactory.retrieveFromDatabase(
        dag.getEdgeIds().get(0));

    super.postgresClient.commit();

    assertEquals(0, successor.getFromId());
    assertEquals(newEdgeVersionId, successor.getToId());
  }
}
