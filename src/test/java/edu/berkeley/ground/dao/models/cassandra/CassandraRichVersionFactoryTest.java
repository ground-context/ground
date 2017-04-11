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

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import edu.berkeley.ground.dao.CassandraTest;
import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.model.models.RichVersion;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class CassandraRichVersionFactoryTest extends CassandraTest {

  private TestCassandraRichVersionFactory richVersionFactory;

  private class TestCassandraRichVersionFactory extends CassandraRichVersionFactory<RichVersion> {
    private TestCassandraRichVersionFactory(
        CassandraClient cassandraClient,
        CassandraStructureVersionFactory structureVersionFactory,
        CassandraTagFactory tagFactory) {

      super(cassandraClient, structureVersionFactory, tagFactory);
    }

    public Class<RichVersion> getType() {
      return RichVersion.class;
    }

    public RichVersion retrieveFromDatabase(long id) {
      throw new NotImplementedException();
    }
  }

  public CassandraRichVersionFactoryTest() throws GroundException {
    super();

    this.richVersionFactory = new TestCassandraRichVersionFactory(CassandraTest.cassandraClient,
        CassandraTest.getStructureVersionFactory(), CassandraTest.tagFactory);
  }

  @Test
  public void testReference() throws GroundException {
    try {
      long id = 1;
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
    } finally {
      CassandraTest.cassandraClient.abort();
    }
  }

  @Test
  public void testTags() throws GroundException {
    try {
      long id = 1;

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
    } finally {
      CassandraTest.cassandraClient.abort();
    }
  }

  @Test
  public void testStructureVersionConformation() throws GroundException {
    try {
      long id = 1;

      String structureName = "testStructure";
      long structureId = CassandraTest.createStructure(structureName).getId();
      long structureVersionId = CassandraTest.createStructureVersion(structureId).getId();

      Map<String, Tag> tags = CassandraTest.createTags();

      this.richVersionFactory.insertIntoDatabase(id, tags, structureVersionId, null,
          new HashMap<>());

      RichVersion retrieved = this.richVersionFactory.retrieveRichVersionData(id);
      assertEquals(retrieved.getStructureVersionId(), structureVersionId);
    } finally {
      CassandraTest.cassandraClient.abort();
    }
  }

  @Test(expected = GroundException.class)
  public void testStructureVersionFails() throws GroundException {
    try {
      long structureVersionId = -1;
      long id = 1;

      // none of these operations should fail
      try {
        String structureName = "testStructure";
        long structureId = CassandraTest.createStructure(structureName).getId();
        structureVersionId = CassandraTest.createStructureVersion(structureId).getId();
      } catch (GroundException ge) {
        CassandraTest.cassandraClient.abort();

        fail(ge.getMessage());
      }

      Map<String, Tag> tags = new HashMap<>();
      tags.put("intfield", new Tag(-1, "intfield", 1, GroundType.INTEGER));
      tags.put("intfield", new Tag(-1, "strfield", "1", GroundType.STRING));
      tags.put("intfield", new Tag(-1, "boolfield", true, GroundType.BOOLEAN));

      // this should fail
      this.richVersionFactory.insertIntoDatabase(id, tags, structureVersionId, null,
          new HashMap<>());
    } finally {
      CassandraTest.cassandraClient.abort();
    }
  }
}
