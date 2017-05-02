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
import java.util.Map;

import dao.PostgresTest;
import exceptions.GroundVersionNotFoundException;
import models.models.StructureVersion;
import models.versions.GroundType;
import exceptions.GroundException;

import static org.junit.Assert.*;

public class PostgresStructureVersionFactoryTest extends PostgresTest {

  public PostgresStructureVersionFactoryTest() throws GroundException {
    super();
  }


  @Test
  public void testStructureVersionCreation() throws GroundException {
    try {
      String structureName = "testStructure";
      long structureId = PostgresTest.createStructure(structureName).getId();

      Map<String, GroundType> structureVersionAttributes = new HashMap<>();
      structureVersionAttributes.put("intfield", GroundType.INTEGER);
      structureVersionAttributes.put("boolfield", GroundType.BOOLEAN);
      structureVersionAttributes.put("strfield", GroundType.STRING);

      long structureVersionId = PostgresTest.structureVersionFactory.create(
          structureId, structureVersionAttributes, new ArrayList<>()).getId();

      StructureVersion retrieved = PostgresTest.structureVersionFactory
          .retrieveFromDatabase(structureVersionId);

      assertEquals(structureId, retrieved.getStructureId());
      Map<String, GroundType> retrievedAttributes = retrieved.getAttributes();

      for (String key : structureVersionAttributes.keySet()) {
        assert (retrievedAttributes).containsKey(key);
        assertEquals(structureVersionAttributes.get(key), retrievedAttributes.get(key));
      }
    } finally {
      PostgresTest.postgresClient.commit();
    }
  }

  @Test(expected = GroundException.class)
  public void testBadStructureVersion() throws GroundException {
    long id = 1;

    try {
      PostgresTest.structureVersionFactory.retrieveFromDatabase(id);
    } catch (GroundException e) {
      assertEquals(GroundVersionNotFoundException.class, e.getClass());

      throw e;
    } finally {
      PostgresTest.postgresClient.commit();
    }
  }
}
