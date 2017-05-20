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

import static org.junit.Assert.assertTrue;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.core.Node;
import edu.berkeley.ground.common.model.core.NodeVersion;
import edu.berkeley.ground.common.model.version.GroundType;
import edu.berkeley.ground.common.model.version.Tag;
import edu.berkeley.ground.postgres.dao.PostgresTest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class PostgresTagDaoTest extends PostgresTest {

  public PostgresTagDaoTest() throws GroundException {
    super();
  }

  @Test
  public void testGetItemIdsByTag() throws GroundException {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

    long nodeId1 = PostgresTest.nodeDao.create(new Node(0L, null, "test1", tagsMap)).getId();
    long nodeId2 = PostgresTest.nodeDao.create(new Node(0L, null, "test2", tagsMap)).getId();

    List<Long> ids = PostgresTest.tagDao.getItemIdsByTag("testtag");

    assertTrue(ids.contains(nodeId1));
    assertTrue(ids.contains(nodeId2));
  }

  @Test
  public void testGetVersionIdsByTag() throws GroundException {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

    long nodeId = PostgresTest.createNode("testNode").getId();

    NodeVersion nodeVersion1 = new NodeVersion(0L, tagsMap, -1, null, new HashMap<>(), nodeId);
    long nodeVersionId1 = PostgresTest.nodeVersionDao.create(nodeVersion1, new ArrayList<>())
                            .getId();
    NodeVersion nodeVersion2 = new NodeVersion(0L, tagsMap, -1, null, new HashMap<>(), nodeId);
    long nodeVersionId2 = PostgresTest.nodeVersionDao.create(nodeVersion2, new ArrayList<>())
                            .getId();

    List<Long> ids = PostgresTest.tagDao.getVersionIdsByTag("testtag");

    assertTrue(ids.contains(nodeVersionId1));
    assertTrue(ids.contains(nodeVersionId2));
  }
}
