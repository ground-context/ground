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

package edu.berkeley.ground.dao.usage.cassandra;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.berkeley.ground.dao.CassandraTest;
import edu.berkeley.ground.model.usage.LineageGraph;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.versions.VersionHistoryDag;
import edu.berkeley.ground.model.versions.VersionSuccessor;

import static org.junit.Assert.assertEquals;

public class CassandraLineageGraphFactoryTest extends CassandraTest {

  public CassandraLineageGraphFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageGraphCreation() throws GroundException {
    String testName = "test";
    String sourceKey = "testKey";

    CassandraLineageGraphFactory graphFactory = (CassandraLineageGraphFactory)
        CassandraTest.factories.getLineageGraphFactory();
    graphFactory.create(testName, sourceKey, new HashMap<>());

    LineageGraph graph = graphFactory.retrieveFromDatabase(testName);

    assertEquals(testName, graph.getName());
    assertEquals(sourceKey, graph.getSourceKey());
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadLineageGraph() throws GroundException {
    String testName = "test";

    try {
      CassandraTest.factories.getLineageGraphFactory().retrieveFromDatabase(testName);
    } catch (GroundException e) {
      assertEquals("No LineageGraph found with name " + testName + ".", e.getMessage());

      throw e;
    }
  }

  @Test(expected = GroundException.class)
  public void testBadLineageGraphVersion() throws GroundException {
    long id = 1;

    try {
      CassandraTest.factories.getLineageGraphVersionFactory().retrieveFromDatabase(id);
    } catch (GroundException e) {
      assertEquals("No RichVersion found with id " + id + ".", e.getMessage());

      throw e;
    }
  }

  @Test
  public void testTruncate() throws GroundException {
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

    String lineageEdgeName = "testLineageEdge";
    long lineageEdgeId = CassandraTest.factories.getLineageEdgeFactory().create(lineageEdgeName,
        null, new HashMap<>()).getId();
    long lineageEdgeVersionId = CassandraTest.factories.getLineageEdgeVersionFactory().create(
        new HashMap<>(), -1, null, new HashMap<>(), lineageEdgeId, firstNodeVersionId,
        secondNodeVersionId, new ArrayList<>()).getId();

    List<Long> lineageEdgeVersionIds = new ArrayList<>();
    lineageEdgeVersionIds.add(lineageEdgeVersionId);

    String lineageGraphName = "testLineageGraph";
    long lineageGraphId = CassandraTest.factories.getLineageGraphFactory().create
        (lineageGraphName, null, new HashMap<>()).getId();

    long lineageGraphVersionId = CassandraTest.factories.getLineageGraphVersionFactory().create(
        new HashMap<>(), -1, null, new HashMap<>(), lineageGraphId, lineageEdgeVersionIds,
        new ArrayList<>()).getId();

    List<Long> parents = new ArrayList<>();
    parents.add(lineageGraphVersionId);

    long newLineageGraphVersionId = CassandraTest.factories.getLineageGraphVersionFactory().create(
        new HashMap<>(), -1, null, new HashMap<>(), lineageGraphId, lineageEdgeVersionIds,
        parents).getId();

    CassandraTest.factories.getLineageGraphFactory().truncate(lineageGraphId, 1);

    VersionHistoryDag<?> dag = CassandraTest.versionHistoryDAGFactory
        .retrieveFromDatabase(lineageGraphId);

    assertEquals(1, dag.getEdgeIds().size());

    VersionSuccessor<?> successor = CassandraTest.versionSuccessorFactory.retrieveFromDatabase(
        dag.getEdgeIds().get(0));

    CassandraTest.cassandraClient.commit();

    assertEquals(0, successor.getFromId());
    assertEquals(newLineageGraphVersionId, successor.getToId());
  }
}
