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
import models.models.Graph;
import exceptions.GroundException;
import models.versions.VersionHistoryDag;
import models.versions.VersionSuccessor;

import static org.junit.Assert.*;

public class Neo4jGraphFactoryTest extends Neo4jTest {

  public Neo4jGraphFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testGraphCreation() throws GroundException {
    try {
      String testName = "test";
      String sourceKey = "testKey";

      Neo4jTest.graphFactory.create(testName, sourceKey, new HashMap<>());
      Graph graph = Neo4jTest.graphFactory.retrieveFromDatabase(sourceKey);

      assertEquals(testName, graph.getName());
      assertEquals(sourceKey, graph.getSourceKey());
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadGraph() throws GroundException {
    String sourceKey = "test";

    try {
      Neo4jTest.graphFactory.retrieveFromDatabase(sourceKey);
    } catch (GroundException e) {
      assertEquals(GroundItemNotFoundException.class, e.getClass());
      throw e;
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testCreateDuplicateGraph() throws GroundException {
    String graphName = "graphName";
    String graphKey = "graphKey";

    try {
      try {
        Neo4jTest.graphFactory.create(graphName, graphKey, new HashMap<>());
      } catch (GroundException e) {
        fail(e.getMessage());
      }

      Neo4jTest.graphFactory.create(graphName, graphKey, new HashMap<>());
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }

  @Test
  public void testTruncate() throws GroundException {
    try {
      long edgeVersionId = Neo4jTest.createTwoNodesAndEdge();

      List<Long> edgeVersionIds = new ArrayList<>();
      edgeVersionIds.add(edgeVersionId);

      String graphName = "testGraph";
      long graphId = Neo4jTest.createGraph(graphName).getId();
      long graphVersionId = Neo4jTest.createGraphVersion(graphId, edgeVersionIds).getId();

      List<Long> parents = new ArrayList<>();
      parents.add(graphVersionId);
      long newGraphVersionId = Neo4jTest.createGraphVersion(graphId, edgeVersionIds, parents)
          .getId();

      Neo4jTest.graphFactory.truncate(graphId, 1);

      VersionHistoryDag<?> dag = Neo4jTest.versionHistoryDAGFactory.retrieveFromDatabase(graphId);

      assertEquals(1, dag.getEdgeIds().size());

      VersionSuccessor<?> successor = Neo4jTest.versionSuccessorFactory.retrieveFromDatabase(
          dag.getEdgeIds().get(0));

      assertEquals(graphId, successor.getFromId());
      assertEquals(newGraphVersionId, successor.getToId());
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }
}
