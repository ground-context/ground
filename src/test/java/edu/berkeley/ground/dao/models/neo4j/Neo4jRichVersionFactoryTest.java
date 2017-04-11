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

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import edu.berkeley.ground.dao.Neo4jTest;
import edu.berkeley.ground.dao.models.RichVersionFactory;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.model.models.RichVersion;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class Neo4jRichVersionFactoryTest extends Neo4jTest {

  private TestNeo4jRichVersionFactory richVersionFactory;

  private class TestNeo4jRichVersionFactory extends Neo4jRichVersionFactory<RichVersion> {
    private TestNeo4jRichVersionFactory(Neo4jClient neo4jClient,
                                       Neo4jStructureVersionFactory structureVersionFactory,
                                       Neo4jTagFactory tagFactory) {

      super(neo4jClient, structureVersionFactory, tagFactory);
    }

    public Class<RichVersion> getType() {
      return RichVersion.class;
    }

    public RichVersion retrieveFromDatabase(long id) {
      throw new NotImplementedException();
    }
  }

  public Neo4jRichVersionFactoryTest() throws GroundException {
    super();

    this.richVersionFactory = new TestNeo4jRichVersionFactory(Neo4jTest.neo4jClient,
        Neo4jTest.getStructureVersionFactory(), Neo4jTest.tagFactory);
  }

  @Test
  public void testReference() throws GroundException {
    try {
      /* Create a NodeVersion because Neo4j's rich version factory looks for an existing version
         with this id */

      long testNodeId = Neo4jTest.createNode("testNode").getId();
      long id = Neo4jTest.createNodeVersion(testNodeId).getId();

      String testReference = "http://www.google.com";
      Map<String, String> parameters = new HashMap<>();
      parameters.put("http", "GET");
      parameters.put("ftp", "test");

      this.richVersionFactory.insertIntoDatabase(id, new HashMap<>(), -1, testReference,
          parameters);

      RichVersion retrieved = this.richVersionFactory.retrieveRichVersionData(id);

      assertEquals(id, retrieved.getId());
      assertEquals(testReference, retrieved.getReference());

      Map<String, String> retrievedParams = retrieved.getParameters();
      for (String key : parameters.keySet()) {
        assert (retrievedParams).containsKey(key);
        assertEquals(parameters.get(key), retrievedParams.get(key));
      }

      Neo4jTest.neo4jClient.commit();
    } finally {
      Neo4jTest.neo4jClient.abort();
    }
  }

  @Test
  public void testTags() throws GroundException {
    try {
      /* Create a NodeVersion because Neo4j's rich version factory looks for an existing version
         with this id */
      long testNodeId = Neo4jTest.createNode("testNode").getId();
      long id = Neo4jTest.createNodeVersion(testNodeId).getId();

      Map<String, Tag> tags = new HashMap<>();
      tags.put("justkey", new Tag(-1, "justkey", null, null));
      tags.put("withintvalue", new Tag(-1, "withintvalue", 1, GroundType.INTEGER));
      tags.put("withstringvalue", new Tag(-1, "withstringvalue", "1", GroundType.STRING));
      tags.put("withboolvalue", new Tag(-1, "withboolvalue", true, GroundType.BOOLEAN));

      this.richVersionFactory.insertIntoDatabase(id, tags, -1, null, new HashMap<>());

      RichVersion retrieved = this.richVersionFactory.retrieveRichVersionData(id);

      assertEquals(id, retrieved.getId());
      assertEquals(tags.size(), retrieved.getTags().size());

      Map<String, Tag> retrievedTags = retrieved.getTags();
      for (String key : tags.keySet()) {
        assert (retrievedTags).containsKey(key);
        assertEquals(tags.get(key), retrievedTags.get(key));
        assertEquals(retrieved.getId(), retrievedTags.get(key).getId());
      }

      Neo4jTest.neo4jClient.commit();
    } finally {
      Neo4jTest.neo4jClient.abort();
    }
  }

  @Test
  public void testStructureVersionConformation() throws GroundException {
    try {
      /* Create a NodeVersion because Neo4j's rich version factory looks for an existing version
      with this id */
      long testNodeId = Neo4jTest.createNode("testNode").getId();
      long id = Neo4jTest.createNodeVersion(testNodeId).getId();

      String structureName = "testStructure";
      long structureId = Neo4jTest.createStructure(structureName).getId();
      long structureVersionId = Neo4jTest.createStructureVersion(structureId).getId();

      Map<String, Tag> tags = Neo4jTest.createTags();

      this.richVersionFactory.insertIntoDatabase(id, tags, structureVersionId, null,
          new HashMap<>());

      RichVersion retrieved = this.richVersionFactory.retrieveRichVersionData(id);
      assertEquals(retrieved.getStructureVersionId(), structureVersionId);

      Neo4jTest.neo4jClient.commit();
    } finally {
      Neo4jTest.neo4jClient.abort();
    }
  }

  @Test(expected = GroundException.class)
  public void testStructureVersionFails() throws GroundException {
    long structureVersionId = -1;

    try {
      /* Create a NodeVersion because Neo4j's rich version factory looks for an existing version
      with this id */
      long testNodeId = Neo4jTest.createNode("testNode").getId();
      long id = Neo4jTest.createNodeVersion(testNodeId).getId();

      // none of these operations should fail
      try {
        String structureName = "testStructure";
        long structureId = Neo4jTest.createStructure(structureName).getId();
        structureVersionId = Neo4jTest.createStructureVersion(structureId).getId();
      } catch (GroundException ge) {
        fail(ge.getMessage());
      }

      Map<String, Tag> tags = new HashMap<>();
      tags.put("boolfield", new Tag(-1, "boolfield", true, GroundType.BOOLEAN));

      // this should fail
      this.richVersionFactory.insertIntoDatabase(id, tags, structureVersionId, null,
          new HashMap<>());

      Neo4jTest.neo4jClient.commit();
    } finally {
      Neo4jTest.neo4jClient.abort();
    }
  }
}
