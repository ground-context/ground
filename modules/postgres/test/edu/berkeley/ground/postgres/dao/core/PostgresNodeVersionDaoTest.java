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
import edu.berkeley.ground.common.model.core.NodeVersion;
import edu.berkeley.ground.common.model.version.Tag;
import edu.berkeley.ground.postgres.dao.PostgresTest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class PostgresNodeVersionDaoTest extends PostgresTest {

  public PostgresNodeVersionDaoTest() throws GroundException {
    super();
  }

  @Test
  public void testNodeVersionCreation() throws GroundException {
    String nodeName = "testNode";
    long nodeId = PostgresTest.createNode(nodeName).getId();

    String structureName = "testStructure";
    long structureId = PostgresTest.createStructure(structureName).getId();
    long structureVersionId = PostgresTest.createStructureVersion(structureId).getId();

    Map<String, Tag> tags = PostgresTest.createTags();

    String testReference = "http://www.google.com";
    Map<String, String> parameters = new HashMap<>();
    parameters.put("http", "GET");

    NodeVersion nodeVersion = new NodeVersion(0L, tags, structureVersionId, testReference, parameters, nodeId);
    long nodeVersionId = PostgresTest.nodeVersionDao.create(nodeVersion, new ArrayList<>()).getId();

    NodeVersion retrieved = PostgresTest.nodeVersionDao.retrieveFromDatabase(nodeVersionId);

    assertEquals(nodeId, retrieved.getNodeId());
    assertEquals(structureVersionId, (long) retrieved.getStructureVersionId());
    assertEquals(testReference, retrieved.getReference());

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

    List<Long> leaves = PostgresTest.nodeDao.getLeaves(nodeName);

    assertTrue(leaves.contains(nodeVersionId));
    assertTrue(1 == leaves.size());
  }

  @Test(expected = GroundException.class)
  public void testBadNodeVersion() throws GroundException {
    long id = 1;

    try {
      PostgresTest.nodeVersionDao.retrieveFromDatabase(id);
    } catch (GroundException e) {
      assertEquals(GroundException.class, e.getClass());

      throw e;
    }
  }
}
