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
import edu.berkeley.ground.exceptions.GroundItemNotFoundException;
import edu.berkeley.ground.model.usage.LineageEdge;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.versions.VersionHistoryDag;
import edu.berkeley.ground.model.versions.VersionSuccessor;

import static org.junit.Assert.*;

public class CassandraLineageEdgeFactoryTest extends CassandraTest {

  public CassandraLineageEdgeFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageEdgeCreation() throws GroundException {
    String testName = "test";
    String sourceKey = "testKey";

    CassandraTest.lineageEdgesResource.createLineageEdge(testName, sourceKey, new HashMap<>());
    LineageEdge lineageEdge = CassandraTest.lineageEdgesResource.getLineageEdge(sourceKey);

    assertEquals(testName, lineageEdge.getName());
    assertEquals(sourceKey, lineageEdge.getSourceKey());
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadLineageEdge() throws GroundException {
    String sourceKey = "test";

    try {
      CassandraTest.lineageEdgesResource.getLineageEdge(sourceKey);
    } catch (GroundException e) {
      assertEquals(GroundItemNotFoundException.class, e.getClass());

      throw e;
    }
  }

  @Test(expected = GroundException.class)
  public void testCreateDuplicateLineageEdge() throws GroundException {
    String lineageEdgeName = "lineageEdgeName";
    String lineageEdgeKey = "lineageEdgeKey";

    try {
      CassandraTest.lineageEdgesResource.createLineageEdge(lineageEdgeName, lineageEdgeKey, new HashMap<>());
    } catch (GroundException e) {
      fail(e.getMessage());
    }

    CassandraTest.lineageEdgesResource.createLineageEdge(lineageEdgeName, lineageEdgeKey, new HashMap<>());
  }

  @Test
  public void testTruncate() throws GroundException {
    String firstTestNode = "firstTestNode";
    long firstTestNodeId = CassandraTest.createNode(firstTestNode).getId();
    long firstNodeVersionId = CassandraTest.createNodeVersion(firstTestNodeId).getId();

    String secondTestNode = "secondTestNode";
    long secondTestNodeId = CassandraTest.createNode(secondTestNode).getId();
    long secondNodeVersionId = CassandraTest.createNodeVersion(secondTestNodeId).getId();

    String lineageEdgeName = "testLineageEdge";
    long lineageEdgeId = CassandraTest.createLineageEdge(lineageEdgeName).getId();
    long lineageEdgeVersionId = CassandraTest.createLineageEdgeVersion(lineageEdgeId,
        firstNodeVersionId, secondNodeVersionId).getId();

    List<Long> parents = new ArrayList<>();
    parents.add(lineageEdgeVersionId);
    long newLineageEdgeVersionId = CassandraTest.createLineageEdgeVersion(lineageEdgeId,
        firstNodeVersionId, secondNodeVersionId, parents).getId();

    CassandraTest.lineageEdgesResource.truncateLineageEdge(lineageEdgeName, 1);

    VersionHistoryDag<?> dag = CassandraTest.versionHistoryDAGFactory
        .retrieveFromDatabase(lineageEdgeId);

    assertEquals(1, dag.getEdgeIds().size());

    VersionSuccessor<?> successor = CassandraTest.versionSuccessorFactory.retrieveFromDatabase(
        dag.getEdgeIds().get(0));

    CassandraTest.cassandraClient.commit();

    assertEquals(0, successor.getFromId());
    assertEquals(newLineageEdgeVersionId, successor.getToId());
  }
}
