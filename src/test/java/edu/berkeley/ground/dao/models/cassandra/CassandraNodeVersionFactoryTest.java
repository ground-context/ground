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

package edu.berkeley.ground.dao.models.cassandra;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.berkeley.ground.dao.CassandraTest;
import edu.berkeley.ground.model.models.NodeVersion;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class CassandraNodeVersionFactoryTest extends CassandraTest {

  public CassandraNodeVersionFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testNodeVersionCreation() throws GroundException {
    String nodeName = "testNode";
    long nodeId = CassandraTest.factories.getNodeFactory()
        .create(nodeName, null, new HashMap<>()).getId();

    String structureName = "testStructure";
    long structureId = CassandraTest.factories.getStructureFactory()
        .create(structureName, null, new HashMap<>()).getId();

    Map<String, GroundType> structureVersionAttributes = new HashMap<>();
    structureVersionAttributes.put("intfield", GroundType.INTEGER);
    structureVersionAttributes.put("boolfield", GroundType.BOOLEAN);
    structureVersionAttributes.put("strfield", GroundType.STRING);

    long structureVersionId = CassandraTest.factories.getStructureVersionFactory().create(
        structureId, structureVersionAttributes, new ArrayList<>()).getId();

    Map<String, Tag> tags = new HashMap<>();
    tags.put("intfield", new Tag(-1, "intfield", 1, GroundType.INTEGER));
    tags.put("strfield", new Tag(-1, "strfield", "1", GroundType.STRING));
    tags.put("boolfield", new Tag(-1, "boolfield", true, GroundType.BOOLEAN));

    String testReference = "http://www.google.com";
    Map<String, String> parameters = new HashMap<>();
    parameters.put("http", "GET");

    long nodeVersionId = CassandraTest.factories.getNodeVersionFactory().create(tags,
        structureVersionId, testReference, parameters, nodeId, new ArrayList<>()).getId();

    NodeVersion retrieved = CassandraTest.factories.
        getNodeVersionFactory().
        retrieveFromDatabase(nodeVersionId);

    assertEquals(nodeId, retrieved.getNodeId());
    assertEquals(structureVersionId, retrieved.getStructureVersionId());
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

    List<Long> leaves = CassandraTest.factories.getNodeFactory().getLeaves(nodeName);

    assertTrue(leaves.contains(nodeVersionId));
    assertTrue(1 == leaves.size());
  }

  @Test(expected = GroundException.class)
  public void testBadNodeVersion() throws GroundException {
    long id = 1;

    try {
      CassandraTest.factories.getNodeVersionFactory().retrieveFromDatabase(id);
    } catch (GroundException e) {
      assertEquals("No RichVersion found with id " + id + ".", e.getMessage());

      throw e;
    }
  }
}
