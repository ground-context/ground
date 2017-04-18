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

package edu.berkeley.ground.dao.models.neo4j;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.berkeley.ground.dao.Neo4jTest;
import edu.berkeley.ground.exceptions.GroundVersionNotFoundException;
import edu.berkeley.ground.model.models.EdgeVersion;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class Neo4jEdgeVersionFactoryTest extends Neo4jTest {

  public Neo4jEdgeVersionFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testEdgeVersionCreation() throws GroundException {
    String firstTestNode = "firstTestNode";
    long firstTestNodeId = Neo4jTest.createNode(firstTestNode).getId();
    long firstNodeVersionId = Neo4jTest.createNodeVersion(firstTestNodeId).getId();

    String secondTestNode = "secondTestNode";
    long secondTestNodeId = Neo4jTest.createNode(secondTestNode).getId();
    long secondNodeVersionId = Neo4jTest.createNodeVersion(secondTestNodeId).getId();

    String edgeName = "testEdge";
    long edgeId = Neo4jTest.createEdge(edgeName, firstTestNode, secondTestNode).getId();

    String structureName = "testStructure";
    long structureId = Neo4jTest.createStructure(structureName).getId();
    long structureVersionId = Neo4jTest.createStructureVersion(structureId).getId();

    Map<String, Tag> tags = Neo4jTest.createTags();

    String testReference = "http://www.google.com";
    Map<String, String> parameters = new HashMap<>();
    parameters.put("http", "GET");

    long edgeVersionId = Neo4jTest.edgesResource.createEdgeVersion(edgeId, tags, parameters,
        structureVersionId, testReference, firstNodeVersionId, -1, secondNodeVersionId, -1,
        new ArrayList<>()).getId();

    EdgeVersion retrieved = Neo4jTest.edgesResource.getEdgeVersion(edgeVersionId);

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
    long firstTestNodeId = Neo4jTest.createNode(firstTestNode).getId();
    long firstNodeVersionId = Neo4jTest.createNodeVersion(firstTestNodeId).getId();

    String secondTestNode = "secondTestNode";
    long secondTestNodeId = Neo4jTest.createNode(secondTestNode).getId();
    long secondNodeVersionId = Neo4jTest.createNodeVersion(secondTestNodeId).getId();

    String edgeName = "testEdge";
    long edgeId = Neo4jTest.createEdge(edgeName, firstTestNode, secondTestNode).getId();
    long edgeVersionId = Neo4jTest.createEdgeVersion(edgeId, firstNodeVersionId,
        secondNodeVersionId).getId();

    EdgeVersion retrieved =  Neo4jTest.edgesResource.getEdgeVersion(edgeVersionId);

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
    long fromEndId = Neo4jTest.createNodeVersion(firstTestNodeId, parents).getId();

    parents.clear();
    parents.add(fromEndId);
    long newFirstNodeVersionId = Neo4jTest.createNodeVersion(firstTestNodeId, parents).getId();

    parents.clear();
    parents.add(secondNodeVersionId);
    long toEndId = Neo4jTest.createNodeVersion(secondTestNodeId, parents).getNodeId();

    parents.clear();
    parents.add(toEndId);
    long newSecondNodeVersionId = Neo4jTest.createNodeVersion(secondTestNodeId, parents)
        .getId();

    parents.clear();
    parents.add(edgeVersionId);
    long newEdgeVersionId = Neo4jTest.createEdgeVersion(edgeId, newFirstNodeVersionId,
        newSecondNodeVersionId, parents).getId();

    EdgeVersion parent = Neo4jTest.edgesResource.getEdgeVersion(edgeVersionId);
    EdgeVersion child = Neo4jTest.edgesResource.getEdgeVersion(newEdgeVersionId);

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
      Neo4jTest.edgesResource.getEdgeVersion(id);
    } catch (GroundException e) {
      assertEquals(GroundVersionNotFoundException.class, e.getClass());

      throw e;
    }
  }
}
