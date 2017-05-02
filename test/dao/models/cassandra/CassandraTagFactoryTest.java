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

package dao.models.cassandra;

import dao.CassandraTest;
import exceptions.GroundException;
import models.models.Tag;
import models.versions.GroundType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class CassandraTagFactoryTest extends CassandraTest {

  public CassandraTagFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testGetItemIdsByTag() throws GroundException {
    Map<String, Tag> tagsMap = new HashMap<>();
    Tag tag = new Tag(1, "testtag", "tag", GroundType.STRING);
    tagsMap.put("testtag", tag);

    long nodeId1 = CassandraTest.nodeFactory.create(null, "test1", tagsMap).getId();
    long nodeId2 = CassandraTest.nodeFactory.create(null, "test2", tagsMap).getId();

    List<Long> ids = CassandraTest.tagFactory.getItemIdsByTag(tag.getKey());

    CassandraTest.cassandraClient.commit();

    assertTrue(ids.contains(nodeId1));
    assertTrue(ids.contains(nodeId2));
  }

  @Test
  public void testGetVersionIdsByTag() throws GroundException {
    Map<String, Tag> tagsMap = new HashMap<>();
    Tag tag = new Tag(1, "testtag", "tag", GroundType.STRING);
    tagsMap.put("testtag", tag);

    long nodeId = CassandraTest.createNode("testNode").getId();

    long nodeVersionId1 = CassandraTest.nodeVersionFactory.create(tagsMap, -1, null,
        new HashMap<>(), nodeId, new ArrayList<>()).getId();
    long nodeVersionId2 = CassandraTest.nodeVersionFactory.create(tagsMap, -1, null,
        new HashMap<>(), nodeId, new ArrayList<>()).getId();

    List<Long> ids = CassandraTest.tagFactory.getVersionIdsByTag(tag.getKey());

    CassandraTest.cassandraClient.commit();

    assertTrue(ids.contains(nodeVersionId1));
    assertTrue(ids.contains(nodeVersionId2));
  }
}
