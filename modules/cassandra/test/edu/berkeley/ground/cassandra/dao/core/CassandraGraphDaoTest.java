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

package edu.berkeley.ground.cassandra.dao.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.core.Graph;
import edu.berkeley.ground.common.model.version.VersionHistoryDag;
import edu.berkeley.ground.common.model.version.VersionSuccessor;
import edu.berkeley.ground.cassandra.dao.CassandraTest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.junit.Test;

public class CassandraGraphDaoTest extends CassandraTest {

  public CassandraGraphDaoTest() throws GroundException {
    super();
  }


  @Test
  public void testGraphCreation() throws GroundException {
    String testName = "test";
    String sourceKey = "testKey";

    CassandraTest.graphDao.create(new Graph(0L, testName, sourceKey, new HashMap<>()));
    Graph graph = CassandraTest.graphDao.retrieveFromDatabase(sourceKey);

    assertEquals(testName, graph.getName());
    assertEquals(sourceKey, graph.getSourceKey());
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadGraph() throws GroundException {
    String sourceKey = "test";

    try {
      CassandraTest.graphDao.retrieveFromDatabase(sourceKey);
    } catch (GroundException e) {
      assertEquals(GroundException.class, e.getClass());

      throw e;
    }
  }

  @Test(expected = GroundException.class)
  public void testCreateDuplicateGraph() throws GroundException {
    String graphName = "graphName";
    String graphKey = "graphKey";

    try {
      CassandraTest.graphDao.create(new Graph(0L, graphName, graphKey, new HashMap<>()));
    } catch (GroundException e) {
      fail(e.getMessage());
    }

    CassandraTest.graphDao.create(new Graph(0L, graphName, graphKey, new HashMap<>()));
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
    long newGraphVersionId = CassandraTest.createGraphVersion(graphId, edgeVersionIds, parents).getId();

    CassandraTest.graphDao.truncate(graphId, 1);

    VersionHistoryDag dag = CassandraTest.versionHistoryDagDao.retrieveFromDatabase(graphId);

    assertEquals(1, dag.getEdgeIds().size());

    VersionSuccessor successor = CassandraTest.versionSuccessorDao.retrieveFromDatabase(dag.getEdgeIds().get(0));

    assertEquals(0, successor.getFromId());
    assertEquals(newGraphVersionId, successor.getToId());
  }
}
