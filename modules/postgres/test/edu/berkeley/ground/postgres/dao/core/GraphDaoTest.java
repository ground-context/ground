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

package edu.berkeley.ground.postgres.dao.core;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.core.Graph;
import edu.berkeley.ground.common.model.version.VersionHistoryDag;
import edu.berkeley.ground.common.model.version.VersionSuccessor;
import edu.berkeley.ground.postgres.dao.PostgresTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;

public class GraphDaoTest extends PostgresTest {

  public GraphDaoTest() throws GroundException {
    super();
  }


  @Test
  public void testGraphCreation() throws GroundException {
    try {
      String testName = "test";
      String sourceKey = "testKey";

      PostgresTest.graphDao.create(new Graph(0L, testName, sourceKey, new HashMap<>()));
      Graph graph = PostgresTest.graphDao.retrieveFromDatabase(sourceKey);

      assertEquals(testName, graph.getName());
      assertEquals(sourceKey, graph.getSourceKey());
    } finally {
      //PostgresTest.postgresClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadGraph() throws GroundException {
    String sourceKey = "test";

    try {
      PostgresTest.graphDao.retrieveFromDatabase(sourceKey);
    } catch (GroundException e) {
      assertEquals(GroundException.class, e.getClass());

      throw e;
    } finally {
      //PostgresTest.postgresClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testCreateDuplicateGraph() throws GroundException {
    String graphName = "graphName";
    String graphKey = "graphKey";

    try {
      try {
        PostgresTest.graphDao.create(new Graph(0L,graphName, graphKey, new HashMap<>()));
      } catch (GroundException e) {
        fail(e.getMessage());
      }

      PostgresTest.graphDao.create(new Graph(0L, graphName, graphKey, new HashMap<>()));
    } finally {
      //PostgresTest.postgresClient.commit();
    }
  }

  @Test
  public void testTruncate() throws GroundException {
    try {
      long edgeVersionId = PostgresTest.createTwoNodesAndEdge();

      List<Long> edgeVersionIds = new ArrayList<>();
      edgeVersionIds.add(edgeVersionId);

      String graphName = "testGraph";
      long graphId = PostgresTest.createGraph(graphName).getId();

      long graphVersionId = PostgresTest.createGraphVersion(graphId, edgeVersionIds).getId();

      List<Long> parents = new ArrayList<>();
      parents.add(graphVersionId);
      long newGraphVersionId = PostgresTest.createGraphVersion(graphId, edgeVersionIds, parents)
        .getId();

      PostgresTest.graphDao.truncate(graphId, 1);

      VersionHistoryDag<?> dag = PostgresTest.versionHistoryDagDao
        .retrieveFromDatabase(graphId);

      assertEquals(1, dag.getEdgeIds().size());

      VersionSuccessor<?> successor = PostgresTest.versionSuccessorDao.retrieveFromDatabase(
        dag.getEdgeIds().get(0));

      //PostgresTest.postgresClient.commit();

      assertEquals(0, successor.getFromId());
      assertEquals(newGraphVersionId, successor.getToId());
    } finally {
      //PostgresTest.postgresClient.commit();
    }
  }
}
