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
import java.util.Map;

import edu.berkeley.ground.dao.CassandraTest;
import edu.berkeley.ground.exceptions.GroundVersionNotFoundException;
import edu.berkeley.ground.model.models.StructureVersion;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class CassandraStructureVersionFactoryTest extends CassandraTest {

  public CassandraStructureVersionFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testStructureVersionCreation() throws GroundException {
    String structureName = "testStructure";
    long structureId = CassandraTest.createStructure(structureName).getId();

    Map<String, GroundType> structureVersionAttributes = new HashMap<>();
    structureVersionAttributes.put("intfield", GroundType.INTEGER);
    structureVersionAttributes.put("boolfield", GroundType.BOOLEAN);
    structureVersionAttributes.put("strfield", GroundType.STRING);

    long structureVersionId = CassandraTest.structuresResource.createStructureVersion(
        structureId, structureVersionAttributes, new ArrayList<>()).getId();

    StructureVersion retrieved = CassandraTest.structuresResource
        .getStructureVersion(structureVersionId);

    assertEquals(structureId, retrieved.getStructureId());
    Map<String, GroundType> retrievedAttributes = retrieved.getAttributes();

    for (String key : structureVersionAttributes.keySet()) {
      assert (retrievedAttributes).containsKey(key);
      assertEquals(structureVersionAttributes.get(key), retrievedAttributes.get(key));
    }
  }

  @Test(expected = GroundException.class)
  public void testBadStructureVersion() throws GroundException {
    long id = 1;

    try {
      CassandraTest.structuresResource.getStructureVersion(id);
    } catch (GroundException e) {
      assertEquals(GroundVersionNotFoundException.class, e.getClass());

      throw e;
    }
  }
}
