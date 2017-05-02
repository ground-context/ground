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

package dao.models.cassandra;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dao.CassandraTest;
import exceptions.GroundVersionNotFoundException;
import models.models.EdgeVersion;
import models.models.Tag;
import models.versions.GroundType;
import exceptions.GroundException;

import static org.junit.Assert.*;

public class CassandraEdgeVersionFactoryTest extends CassandraTest {

  public CassandraEdgeVersionFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testEdgeVersionCreation() throws GroundException {
    String firstTestNode = "firstTestNode";
    long firstTestNodeId = CassandraTest.createNode(firstTestNode).getId();
    long firstNodeVersionId = CassandraTest.createNodeVersion(firstTestNodeId).getId();

    String secondTestNode = "secondTestNode";
    long secondTestNodeId = CassandraTest.createNode(secondTestNode).getId();
    long secondNodeVersionId = CassandraTest.createNodeVersion(secondTestNodeId).getId();

    String edgeName = "testEdge";
    long edgeId = CassandraTest.createEdge(edgeName, firstTestNode, secondTestNode).getId();

    String structureName = "testStructure";
    long structureId = CassandraTest.createStructure(structureName).getId();
    long structureVersionId = CassandraTest.createStructureVersion(structureId).getId();

    Map<String, Tag> tags = CassandraTest.createTags();

    String testReference = "http://www.google.com";
    Map<String, String> parameters = new HashMap<>();
    parameters.put("http", "GET");

    long edgeVersionId = CassandraTest.edgeVersionFactory.create(tags, structureVersionId,
        testReference, parameters, edgeId, firstNodeVersionId, -1, secondNodeVersionId, -1,
        new ArrayList<>()).getId();

    EdgeVersion retrieved = CassandraTest.edgeVersionFactory.retrieveFromDatabase(edgeVersionId);

    assertEquals(edgeId, retrieved.getEdgeId());
    assertEquals(structureVersionId, retrieved.getStructureVersionId());
    assertEquals(testReference, retrieved.getReference());
    assertEquals(firstNodeVersionId, retrieved.getFromNodeVersionStartId());
    assertEquals(secondNodeVersionId, retrieved.getToNodeVersionStartId());
    assertEquals(-1, retrieved.getFromNodeVersionEndId());
    assertEquals(-1, retrieved.getToNodeVersionEndId());

    assertEquals(parameters.size(), retrieved.getParameters().size());
    assertEquals(tags.size(), retrieved.getTags().size());

    Map<String, String> retrievedParameters = retrieved.getParameters();
    Map<String, Tag> retrievedTags = retrieved.getTags();

    for (String key : parameters.keySet()) {
      assert (retrievedParameters).containsKey(key);
      assertEquals(parameters.get(key), retrievedParameters.get(key));
    }

    for (String key : tags.keySet()) {
      assert (retrievedTags).containsKey(key);
      assertEquals(tags.get(key), retrievedTags.get(key));
    }
  }

  @Test
  public void testCorrectEndVersion() throws GroundException {
    String firstTestNode = "firstTestNode";
    long firstTestNodeId = CassandraTest.createNode(firstTestNode).getId();
    long firstNodeVersionId = CassandraTest.createNodeVersion(firstTestNodeId).getId();

    String secondTestNode = "secondTestNode";
    long secondTestNodeId = CassandraTest.createNode(secondTestNode).getId();
    long secondNodeVersionId = CassandraTest.createNodeVersion(secondTestNodeId).getId();

    String edgeName = "testEdge";
    long edgeId = CassandraTest.createEdge(edgeName, firstTestNode, secondTestNode).getId();

    long edgeVersionId = CassandraTest.createEdgeVersion(edgeId, firstNodeVersionId,
        secondNodeVersionId).getId();

    EdgeVersion retrieved = CassandraTest.edgeVersionFactory.retrieveFromDatabase(edgeVersionId);

    assertEquals(edgeId, retrieved.getEdgeId());
    assertEquals(-1, retrieved.getStructureVersionId());
    assertEquals(null, retrieved.getReference());
    assertEquals(firstNodeVersionId, retrieved.getFromNodeVersionStartId());
    assertEquals(secondNodeVersionId, retrieved.getToNodeVersionStartId());
    assertEquals(-1, retrieved.getFromNodeVersionEndId());
    assertEquals(-1, retrieved.getToNodeVersionEndId());

    // create two new node versions in each of the nodes
    List<Long> parents = new ArrayList<>();
    parents.add(firstNodeVersionId);
    long fromEndId = CassandraTest.createNodeVersion(firstTestNodeId, parents).getId();

    parents.clear();
    parents.add(fromEndId);
    long newFirstNodeVersionId = CassandraTest.createNodeVersion(firstTestNodeId, parents).getId();

    parents.clear();
    parents.add(secondNodeVersionId);
    long toEndId = CassandraTest.createNodeVersion(secondTestNodeId, parents).getNodeId();

    parents.clear();
    parents.add(toEndId);
    long newSecondNodeVersionId = CassandraTest.createNodeVersion(secondTestNodeId, parents)
        .getId();

    parents.clear();
    parents.add(edgeVersionId);
    long newEdgeVersionId = CassandraTest.createEdgeVersion(edgeId, newFirstNodeVersionId,
        newSecondNodeVersionId, parents).getId();

    EdgeVersion parent = CassandraTest.edgeVersionFactory.retrieveFromDatabase(edgeVersionId);
    EdgeVersion child = CassandraTest.edgeVersionFactory.retrieveFromDatabase(newEdgeVersionId);

    assertEquals(edgeId, child.getEdgeId());
    assertEquals(-1, child.getStructureVersionId());
    assertEquals(null, child.getReference());
    assertEquals(newFirstNodeVersionId, child.getFromNodeVersionStartId());
    assertEquals(newSecondNodeVersionId, child.getToNodeVersionStartId());
    assertEquals(-1, child.getFromNodeVersionEndId());
    assertEquals(-1, child.getToNodeVersionEndId());

    // Make sure that the end versions were set correctly
    assertEquals(firstNodeVersionId, parent.getFromNodeVersionStartId());
    assertEquals(secondNodeVersionId, parent.getToNodeVersionStartId());
    assertEquals(fromEndId, parent.getFromNodeVersionEndId());
    assertEquals(toEndId, parent.getToNodeVersionEndId());
  }

  @Test(expected = GroundException.class)
  public void testBadEdgeVersion() throws GroundException {
    long id = 1;

    try {
      CassandraTest.edgeVersionFactory.retrieveFromDatabase(id);
    } catch (GroundException e) {
      assertEquals(GroundVersionNotFoundException.class, e.getClass());

      throw e;
    }
  }
}
