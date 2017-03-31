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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.berkeley.ground.dao.Neo4jTest;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class Neo4jTagFactoryTest extends Neo4jTest {

  public Neo4jTagFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testGetItemIdsByTag() throws GroundException {
    Map<String, Tag> tagsMap = new HashMap<>();
    Tag tag = new Tag(1, "testtag", "tag", GroundType.STRING);
    tagsMap.put("testtag", tag);

    long nodeId1 = super.factories.getNodeFactory().create("test1", null, tagsMap).getId();
    long nodeId2 = super.factories.getNodeFactory().create("test2", null, tagsMap).getId();

    List<Long> ids = super.tagFactory.getItemIdsByTag(tag);

    super.neo4jClient.commit();

    assertTrue(ids.contains(nodeId1));
    assertTrue(ids.contains(nodeId2));
  }

  @Test
  public void testGetVersionIdsByTag() throws GroundException {
    Map<String, Tag> tagsMap = new HashMap<>();
    Tag tag = new Tag(1, "testtag", "tag", GroundType.STRING);
    tagsMap.put("testtag", tag);

    long nodeId = super.factories.getNodeFactory().create("test1", null, new HashMap<>()).getId();

    long nodeVersionId1 = super.factories.getNodeVersionFactory().create(tagsMap,
        -1, null, new HashMap<>(), nodeId, new ArrayList<>()).getId();
    long nodeVersionId2 = super.factories.getNodeVersionFactory().create(tagsMap,
        -1, null, new HashMap<>(), nodeId, new ArrayList<>()).getId();

    List<Long> ids = super.tagFactory.getVersionIdsByTag(tag);

    super.neo4jClient.commit();

    assertTrue(ids.contains(nodeVersionId1));
    assertTrue(ids.contains(nodeVersionId2));
  }
}
