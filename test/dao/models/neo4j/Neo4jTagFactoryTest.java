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

package dao.models.neo4j;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import dao.Neo4jTest;
import models.models.Tag;
import models.versions.GroundType;
import exceptions.GroundException;

import static org.junit.Assert.*;

public class Neo4jTagFactoryTest extends Neo4jTest {

  public Neo4jTagFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testGetItemIdsByTag() throws GroundException {
    try {
      Map<String, Tag> tagsMap = new HashMap<>();
      tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

      long nodeId1 = Neo4jTest.nodeFactory.create(null, "test1", tagsMap).getId();
      long nodeId2 = Neo4jTest.nodeFactory.create(null, "test2", tagsMap).getId();

      List<Long> ids = Neo4jTest.tagFactory.getItemIdsByTag("testtag");

      Neo4jTest.neo4jClient.commit();

      assertTrue(ids.contains(nodeId1));
      assertTrue(ids.contains(nodeId2));
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }

  @Test
  public void testGetVersionIdsByTag() throws GroundException {
    try {
      Map<String, Tag> tagsMap = new HashMap<>();
      tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

      long nodeId = Neo4jTest.createNode("testNode").getId();

      long nodeVersionId1 = Neo4jTest.nodeVersionFactory.create(tagsMap, -1, null, new HashMap<>(),
          nodeId, new ArrayList<>()).getId();
      long nodeVersionId2 = Neo4jTest.nodeVersionFactory.create(tagsMap, -1, null, new HashMap<>(),
          nodeId, new ArrayList<>()).getId();

      List<Long> ids = Neo4jTest.tagFactory.getVersionIdsByTag("testtag");

      Neo4jTest.neo4jClient.commit();

      assertTrue(ids.contains(nodeVersionId1));
      assertTrue(ids.contains(nodeVersionId2));
    } finally {
      Neo4jTest.neo4jClient.commit();
    }
  }
}
