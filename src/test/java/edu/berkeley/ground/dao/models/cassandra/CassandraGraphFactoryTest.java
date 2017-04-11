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
import edu.berkeley.ground.model.models.Graph;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.GraphVersion;
import edu.berkeley.ground.model.versions.VersionHistoryDag;
import edu.berkeley.ground.model.versions.VersionSuccessor;

import static org.junit.Assert.*;

public class CassandraGraphFactoryTest extends CassandraTest {

  public CassandraGraphFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testGraphCreation() throws GroundException {
    String testName = "test";
    String sourceKey = "testKey";

    CassandraTest.graphsResource.createGraph(testName, sourceKey, new HashMap<>());
    Graph graph = CassandraTest.graphsResource.getGraph(sourceKey);

    assertEquals(testName, graph.getName());
    assertEquals(sourceKey, graph.getSourceKey());
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadGraph() throws GroundException {
    String sourceKey = "test";

    try {
      CassandraTest.graphsResource.getGraph(sourceKey);
    } catch (GroundException e) {
      assertEquals(GroundItemNotFoundException.class, e.getClass());

      throw e;
    }
  }

  @Test(expected = GroundException.class)
  public void testCreateDuplicateGraph() throws GroundException {
    String graphName = "graphName";
    String graphKey = "graphKey";

    try {
      CassandraTest.graphsResource.createGraph(graphName, graphKey, new HashMap<>());
    } catch (GroundException e) {
      fail(e.getMessage());
    }

    CassandraTest.graphsResource.createGraph(graphName, graphKey, new HashMap<>());
  }


  @Test
  public void testTruncate() throws GroundException {
    long edgeVersionId = CassandraTest.createTwoNodesAndEdge();

    List<Long> edgeVersionIds = new ArrayList<>();
    edgeVersionIds.add(edgeVersionId);

    String graphName = "testGraph";
    long graphId = CassandraTest.createGraph(graphName).getId();

    long graphVersionId = CassandraTest.createGraphVersion(graphId, edgeVersionIds).getId();

    List<Long> parents = new ArrayList<>();
    parents.add(graphVersionId);
    long newGraphVersionId = CassandraTest.createGraphVersion(graphId, edgeVersionIds, parents)
        .getId();

    CassandraTest.graphsResource.truncateGraph(graphName, 1);

    VersionHistoryDag<?> dag = CassandraTest.versionHistoryDAGFactory.retrieveFromDatabase(graphId);

    assertEquals(1, dag.getEdgeIds().size());

    VersionSuccessor<?> successor = CassandraTest.versionSuccessorFactory.retrieveFromDatabase(
        dag.getEdgeIds().get(0));

    CassandraTest.cassandraClient.commit();

    assertEquals(0, successor.getFromId());
    assertEquals(newGraphVersionId, successor.getToId());
  }
}
