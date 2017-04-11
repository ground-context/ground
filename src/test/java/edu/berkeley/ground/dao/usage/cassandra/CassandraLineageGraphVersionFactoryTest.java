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
import java.util.Map;

import edu.berkeley.ground.dao.CassandraTest;
import edu.berkeley.ground.exceptions.GroundVersionNotFoundException;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.usage.LineageGraphVersion;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CassandraLineageGraphVersionFactoryTest extends CassandraTest {

  public CassandraLineageGraphVersionFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageGraphVersionCreation() throws GroundException {
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

    List<Long> lineageEdgeVersionIds = new ArrayList<>();
    lineageEdgeVersionIds.add(lineageEdgeVersionId);

    String lineageGraphName = "testLineageGraph";
    long lineageGraphId = CassandraTest.createLineageGraph(lineageGraphName).getId();

    String structureName = "testStructure";
    long structureId = CassandraTest.createStructure(structureName).getId();
    long structureVersionId = CassandraTest.createStructureVersion(structureId).getId();

    Map<String, Tag> tags = CassandraTest.createTags();

    String testReference = "http://www.google.com";
    Map<String, String> parameters = new HashMap<>();
    parameters.put("http", "GET");

    long lineageGraphVersionId = CassandraTest.lineageGraphsResource
        .createLineageGraphVersion(lineageGraphId, tags, parameters, structureVersionId,
            testReference, lineageEdgeVersionIds, new ArrayList<>()).getId();

    LineageGraphVersion retrieved = CassandraTest.lineageGraphsResource
        .getLineageGraphVersion(lineageGraphVersionId);

    assertEquals(lineageGraphId, retrieved.getLineageGraphId());
    assertEquals(structureVersionId, retrieved.getStructureVersionId());
    assertEquals(testReference, retrieved.getReference());
    assertEquals(lineageEdgeVersionIds.size(), retrieved.getLineageEdgeVersionIds().size());

    List<Long> retrievedLineageEdgeVersionIds = retrieved.getLineageEdgeVersionIds();

    for (long id : lineageEdgeVersionIds) {
      assert (retrievedLineageEdgeVersionIds).contains(id);
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

  @Test(expected = GroundException.class)
  public void testBadLineageGraphVersion() throws GroundException {
    long id = 1;

    try {
      CassandraTest.lineageGraphsResource.getLineageGraphVersion(id);
    } catch (GroundException e) {
      assertEquals(GroundVersionNotFoundException.class, e.getClass());

      throw e;
    }
  }

  @Test
  public void testCreateEmptyLineageGraph() throws GroundException {
    String lineageGraphName = "testGraph";
    long lineageGraphId = CassandraTest.createLineageGraph(lineageGraphName).getId();

    long lineageGraphVersionId = CassandraTest.createLineageGraphVersion(lineageGraphId,
        new ArrayList<>()).getId();

    LineageGraphVersion retrieved = CassandraTest.lineageGraphsResource
        .getLineageGraphVersion(lineageGraphVersionId);

    assertTrue(retrieved.getLineageEdgeVersionIds().isEmpty());
  }
}
