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
import models.models.Tag;
import models.versions.GroundType;
import exceptions.GroundException;

import static org.junit.Assert.*;

public class PostgresTagFactoryTest extends PostgresTest {

  public PostgresTagFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testGetItemIdsByTag() throws GroundException {
    try {
      Map<String, Tag> tagsMap = new HashMap<>();
      tagsMap.put("testtag", new Tag(1, -1, "testtag", "tag", GroundType.STRING));

      long nodeId1 = PostgresTest.nodeFactory.create(null, "test1", tagsMap).getId();
      long nodeId2 = PostgresTest.nodeFactory.create(null, "test2", tagsMap).getId();

      List<Long> ids = PostgresTest.tagFactory.getItemIdsByTag("testtag");

      PostgresTest.postgresClient.commit();

      assertTrue(ids.contains(nodeId1));
      assertTrue(ids.contains(nodeId2));
    } finally {
      PostgresTest.postgresClient.commit();
    }
  }

  @Test
  public void testGetVersionIdsByTag() throws GroundException {
    try {
      Map<String, Tag> tagsMap = new HashMap<>();
      tagsMap.put("testtag", new Tag(1, -1, "testtag", "tag", GroundType.STRING));

      long nodeId = PostgresTest.createNode("testNode").getId();

      long nodeVersionId1 = PostgresTest.nodeVersionFactory
          .create(tagsMap, -1, null, new HashMap<>(),
              nodeId, new ArrayList<>()).getId();
      long nodeVersionId2 = PostgresTest.nodeVersionFactory
          .create(tagsMap, -1, null, new HashMap<>(),
              nodeId, new ArrayList<>()).getId();

      List<Long> ids = PostgresTest.tagFactory.getVersionIdsByTag("testtag");

      PostgresTest.postgresClient.commit();

      assertTrue(ids.contains(nodeVersionId1));
      assertTrue(ids.contains(nodeVersionId2));
    } finally {
      PostgresTest.postgresClient.commit();
    }
  }
}
