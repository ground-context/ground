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
import edu.berkeley.ground.model.models.GraphVersion;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class Neo4jGraphVersionFactoryTest extends Neo4jTest {

  public Neo4jGraphVersionFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testGraphVersionCreation() throws GroundException {
    long edgeVersionId = Neo4jTest.createTwoNodesAndEdge();
    List<Long> edgeVersionIds = new ArrayList<>();
    edgeVersionIds.add(edgeVersionId);

    String graphName = "testGraph";
    long graphId = Neo4jTest.createGraph(graphName).getId();

    String structureName = "testStructure";
    long structureId = Neo4jTest.createStructure(structureName).getId();
    long structureVersionId = Neo4jTest.createStructureVersion(structureId).getId();

    Map<String, Tag> tags = Neo4jTest.createTags();

    String testReference = "http://www.google.com";
    Map<String, String> parameters = new HashMap<>();
    parameters.put("http", "GET");

    long graphVersionId = Neo4jTest.graphsResource.createGraphVersion(graphId, tags, parameters,
        structureVersionId, testReference, edgeVersionIds, new ArrayList<>()).getId();

    GraphVersion retrieved = Neo4jTest.graphsResource.getGraphVersion(graphVersionId);

    assertEquals(graphId, retrieved.getGraphId());
    assertEquals(structureVersionId, retrieved.getStructureVersionId());
    assertEquals(testReference, retrieved.getReference());
    assertEquals(edgeVersionIds.size(), retrieved.getEdgeVersionIds().size());

    List<Long> retrievedEdgeVersionIds = retrieved.getEdgeVersionIds();

    for (long id : edgeVersionIds) {
      assert (retrievedEdgeVersionIds).contains(id);
    }

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
  public void testCreateEmptyGraph() throws GroundException {
    String graphName = "testGraph";
    long graphId = Neo4jTest.createGraph(graphName).getId();
    long graphVersionId = Neo4jTest.createGraphVersion(graphId, new ArrayList<>()).getId();

    GraphVersion retrieved = Neo4jTest.graphsResource.getGraphVersion(graphVersionId);
    assertTrue(retrieved.getEdgeVersionIds().isEmpty());
  }

  @Test(expected = GroundException.class)
  public void testBadGraphVersion() throws GroundException {
    long id = 1;

    try {
      Neo4jTest.graphsResource.getGraphVersion(id);
    } catch (GroundException e) {
      assertEquals(GroundVersionNotFoundException.class, e.getClass());

      throw e;
    }
  }
}
