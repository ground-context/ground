/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.postgres.dao.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.core.GraphVersion;
import edu.berkeley.ground.common.model.version.Tag;
import edu.berkeley.ground.postgres.dao.PostgresTest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class PostgresGraphVersionDaoTest extends PostgresTest {

  public PostgresGraphVersionDaoTest() throws GroundException {
    super();
  }

  @Test
  public void testGraphVersionCreation() throws GroundException {
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

    GraphVersion graphVersion = new GraphVersion(0L, tags, structureVersionId, testReference, parameters, graphId, edgeVersionIds);
    long graphVersionId = PostgresTest.graphVersionDao.create(graphVersion, new ArrayList<>()).getId();

    GraphVersion retrieved = PostgresTest.graphVersionDao.retrieveFromDatabase(graphVersionId);

    assertEquals(graphId, retrieved.getGraphId());
    assertEquals(structureVersionId, (long) retrieved.getStructureVersionId());
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
    long graphId = PostgresTest.createGraph(graphName).getId();

    GraphVersion graphVersion = new GraphVersion(0L, new HashMap<>(), -1, null,
                                                  new HashMap<>(), graphId, new ArrayList<>());
    long graphVersionId = PostgresTest.graphVersionDao.create(graphVersion, new ArrayList<>())
                            .getId();

    GraphVersion retrieved = PostgresTest.graphVersionDao
                               .retrieveFromDatabase(graphVersionId);

    assertTrue(retrieved.getEdgeVersionIds().isEmpty());
  }

  @Test(expected = GroundException.class)
  public void testBadGraphVersion() throws GroundException {
    long id = 1;

    try {
      PostgresTest.graphVersionDao.retrieveFromDatabase(id);
    } catch (GroundException e) {
      assertEquals(GroundException.class, e.getClass());

      throw e;
    }
  }
}
