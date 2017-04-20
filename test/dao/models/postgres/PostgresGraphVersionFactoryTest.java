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

package dao.models.postgres;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dao.PostgresTest;
import exceptions.GroundVersionNotFoundException;
import models.models.GraphVersion;
import models.models.Tag;
import models.versions.GroundType;
import exceptions.GroundException;

import static org.junit.Assert.*;

public class PostgresGraphVersionFactoryTest extends PostgresTest {

  public PostgresGraphVersionFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testGraphVersionCreation() throws GroundException {
    try {
      long edgeVersionId = PostgresTest.createTwoNodesAndEdge();

      List<Long> edgeVersionIds = new ArrayList<>();
      edgeVersionIds.add(edgeVersionId);

      String graphName = "testGraph";
      long graphId = PostgresTest.createGraph(graphName).getId();

      String structureName = "testStructure";
      long structureId = PostgresTest.createStructure(structureName).getId();

      long structureVersionId = PostgresTest.createStructureVersion(structureId).getId();

      Map<String, Tag> tags = PostgresTest.createTags();

      String testReference = "http://www.google.com";
      Map<String, String> parameters = new HashMap<>();
      parameters.put("http", "GET");

      long graphVersionId = PostgresTest.graphVersionFactory.create(tags, structureVersionId,
          testReference, parameters, graphId, edgeVersionIds, new ArrayList<>()).getId();

      GraphVersion retrieved = PostgresTest.graphVersionFactory
          .retrieveFromDatabase(graphVersionId);

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
    } finally {
      PostgresTest.postgresClient.commit();
    }
  }

  @Test
  public void testCreateEmptyGraph() throws GroundException {
    try {
      String graphName = "testGraph";
      long graphId = PostgresTest.createGraph(graphName).getId();

      long graphVersionId = PostgresTest.graphVersionFactory.create(new HashMap<>(), -1, null,
          new HashMap<>(), graphId, new ArrayList<>(), new ArrayList<>()).getId();

      GraphVersion retrieved = PostgresTest.graphVersionFactory
          .retrieveFromDatabase(graphVersionId);

      assertTrue(retrieved.getEdgeVersionIds().isEmpty());
    } finally {
      PostgresTest.postgresClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testBadGraphVersion() throws GroundException {
    long id = 1;

    try {
      PostgresTest.graphVersionFactory.retrieveFromDatabase(id);
    } catch (GroundException e) {
      assertEquals(GroundVersionNotFoundException.class, e.getClass());

      throw e;
    } finally {
      PostgresTest.postgresClient.commit();
    }
  }
}
