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

import edu.berkeley.ground.dao.PostgresTest;
import edu.berkeley.ground.model.models.Graph;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.versions.VersionHistoryDag;
import edu.berkeley.ground.model.versions.VersionSuccessor;

import static org.junit.Assert.*;

public class PostgresGraphFactoryTest extends PostgresTest {

  public PostgresGraphFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testGraphCreation() throws GroundException {
    String testName = "test";
    String sourceKey = "testKey";

    PostgresGraphFactory graphFactory = (PostgresGraphFactory) super.factories.getGraphFactory();
    graphFactory.create(testName, sourceKey, new HashMap<>());

    Graph graph = graphFactory.retrieveFromDatabase(testName);

    assertEquals(testName, graph.getName());
    assertEquals(sourceKey, graph.getSourceKey());
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadGraph() throws GroundException {
    String testName = "test";

    try {
      super.factories.getGraphFactory().retrieveFromDatabase(testName);
    } catch (GroundException e) {
      assertEquals("No Graph found with name " + testName + ".", e.getMessage());

      throw e;
    }
  }

  @Test
  public void testTruncate() throws GroundException {
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

    List<Long> edgeVersionIds = new ArrayList<>();
    edgeVersionIds.add(edgeVersionId);

    String graphName = "testGraph";
    long graphId = super.factories.getGraphFactory().create(graphName, null, new HashMap<>())
        .getId();

    long graphVersionId = super.factories.getGraphVersionFactory().create(new HashMap<>(),
        -1, null, new HashMap<>(), graphId, edgeVersionIds, new ArrayList<>()).getId();

    List<Long> parents = new ArrayList<>();
    parents.add(graphVersionId);
    long newGraphVersionId = super.factories.getGraphVersionFactory().create(
        new HashMap<>(), -1, null, new HashMap<>(), graphId, edgeVersionIds, parents).getId();

    super.factories.getGraphFactory().truncate(graphId, 1);

    VersionHistoryDag<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(graphId);

    assertEquals(1, dag.getEdgeIds().size());

    VersionSuccessor<?> successor = super.versionSuccessorFactory.retrieveFromDatabase(
        dag.getEdgeIds().get(0));

    super.postgresClient.commit();

    assertEquals(0, successor.getFromId());
    assertEquals(newGraphVersionId, successor.getToId());

  }
}
