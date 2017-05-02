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

package dao.usage.cassandra;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import dao.CassandraTest;
import exceptions.GroundVersionNotFoundException;
import models.usage.LineageEdgeVersion;
import models.models.Tag;
import models.versions.GroundType;
import exceptions.GroundException;

import static org.junit.Assert.*;

public class CassandraLineageEdgeVersionFactoryTest extends CassandraTest {

  public CassandraLineageEdgeVersionFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageEdgeVersionCreation() throws GroundException {
    String firstTestNode = "firstTestNode";
    long firstTestNodeId = CassandraTest.createNode(firstTestNode).getId();
    long firstNodeVersionId = CassandraTest.createNodeVersion(firstTestNodeId).getId();

    String secondTestNode = "secondTestNode";
    long secondTestNodeId = CassandraTest.createNode(secondTestNode).getId();
    long secondNodeVersionId = CassandraTest.createNodeVersion(secondTestNodeId).getId();

    String lineageEdgeName = "testLineageEdge";
    long lineageEdgeId = CassandraTest.createLineageEdge(lineageEdgeName).getId();

    String structureName = "testStructure";
    long structureId = CassandraTest.createStructure(structureName).getId();
    long structureVersionId = CassandraTest.createStructureVersion(structureId).getId();

    Map<String, Tag> tags = CassandraTest.createTags();

    String testReference = "http://www.google.com";
    Map<String, String> parameters = new HashMap<>();
    parameters.put("http", "GET");

    long lineageEdgeVersionId = CassandraTest.lineageEdgeVersionFactory.create(
        tags, structureVersionId, testReference, parameters, firstNodeVersionId,
        secondNodeVersionId, lineageEdgeId, new ArrayList<>()).getId();

    LineageEdgeVersion retrieved = CassandraTest.lineageEdgeVersionFactory
        .retrieveFromDatabase(lineageEdgeVersionId);

    assertEquals(lineageEdgeId, retrieved.getLineageEdgeId());
    assertEquals(structureVersionId, retrieved.getStructureVersionId());
    assertEquals(testReference, retrieved.getReference());
    assertEquals(retrieved.getFromId(), firstNodeVersionId);
    assertEquals(retrieved.getToId(), secondNodeVersionId);

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
  public void testBadLineageEdgeVersion() throws GroundException {
    long id = 1;

    try {
      CassandraTest.lineageEdgeVersionFactory.retrieveFromDatabase(id);
    } catch (GroundException e) {
      assertEquals(GroundVersionNotFoundException.class, e.getClass());

      throw e;
    }
  }
}
