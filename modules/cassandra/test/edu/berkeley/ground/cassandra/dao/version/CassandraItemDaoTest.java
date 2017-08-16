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

package edu.berkeley.ground.cassandra.dao.version;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.version.GroundType;
import edu.berkeley.ground.common.model.version.Item;
import edu.berkeley.ground.common.model.version.Tag;
import edu.berkeley.ground.common.model.version.Version;
import edu.berkeley.ground.common.model.version.VersionHistoryDag;
import edu.berkeley.ground.common.model.version.VersionSuccessor;
import edu.berkeley.ground.cassandra.dao.CassandraTest;
import edu.berkeley.ground.cassandra.util.CassandraStatements;
import edu.berkeley.ground.cassandra.util.CassandraUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class CassandraItemDaoTest extends CassandraTest {

  public CassandraItemDaoTest() throws GroundException {
    super();
  }

  @Test
  public void testCorrectUpdateWithParent() throws GroundException {
    long testId = 1;

    Item item = new Item(testId, new HashMap<>());
    CassandraTest.cassandraItemDao.create(item);

    long fromId = 2;
    long toId = 3;

    CassandraUtils.executeCqlList(CassandraTest.dbSource, (CassandraStatements) CassandraTest.cassandraVersionDao.insert(new Version(fromId)));
    CassandraUtils.executeCqlList(CassandraTest.dbSource, (CassandraStatements) CassandraTest.cassandraVersionDao.insert(new Version(toId)));

    List<Long> parentIds = new ArrayList<>();
    CassandraUtils.executeCqlList(dbSource, (CassandraStatements) CassandraTest.cassandraItemDao.update(testId, fromId, parentIds));

    parentIds.clear();
    parentIds.add(fromId);
    CassandraUtils.executeCqlList(dbSource, (CassandraStatements) CassandraTest.cassandraItemDao.update(testId, toId, parentIds));

    VersionHistoryDag dag = CassandraTest.versionHistoryDagDao.retrieveFromDatabase(testId);

    assertEquals(2, dag.getEdgeIds().size());
    assertEquals(toId, (long) dag.getLeaves().get(0));

    VersionSuccessor successor = null;
    for (long id : dag.getEdgeIds()) {
      successor = CassandraTest.versionSuccessorDao.retrieveFromDatabase(id);

      if (successor.getFromId() != 0) {
        break;
      }
    }

    if (successor != null) {
      assertEquals(fromId, successor.getFromId());
      assertEquals(toId, successor.getToId());

      return;
    }

    fail();
  }

  @Test
  public void testCorrectUpdateWithoutParent() throws GroundException {
    long testId = 1;

    CassandraTest.cassandraItemDao.create(new Item(testId, new HashMap<>()));
    long toId = 2;
    CassandraUtils.executeCqlList(CassandraTest.dbSource, (CassandraStatements) CassandraTest.cassandraVersionDao.insert(new Version(toId)));

    List<Long> parentIds = new ArrayList<>();

    // No parent is specified, and there is no other version in this Item, we should
    // automatically make this a child of EMPTY
    CassandraUtils.executeCqlList(dbSource, (CassandraStatements) CassandraTest.cassandraItemDao.update(testId, toId, parentIds));

    VersionHistoryDag dag = CassandraTest.versionHistoryDagDao.retrieveFromDatabase(testId);

    assertEquals(1, dag.getEdgeIds().size());
    assertEquals(toId, (long) dag.getLeaves().get(0));

    VersionSuccessor successor = CassandraTest.versionSuccessorDao.retrieveFromDatabase(
      dag.getEdgeIds().get(0));

    assertEquals(0, successor.getFromId());
    assertEquals(toId, successor.getToId());
  }

  @Test
  public void testCorrectUpdateWithLinearHistory() throws GroundException {
    long testId = 1;

    CassandraTest.cassandraItemDao.create(new Item(testId, new HashMap<>()));

    long fromId = 2;
    long toId = 3;

    CassandraUtils.executeCqlList(CassandraTest.dbSource, (CassandraStatements) CassandraTest.cassandraVersionDao.insert(new Version(fromId)));
    CassandraUtils.executeCqlList(CassandraTest.dbSource, (CassandraStatements) CassandraTest.cassandraVersionDao.insert(new Version(toId)));
    List<Long> parentIds = new ArrayList<>();

    // first, make from a child of EMPTY
    CassandraUtils.executeCqlList(dbSource, (CassandraStatements) CassandraTest.cassandraItemDao.update(testId, fromId, parentIds));

    // then, add to as a child and make sure that it becomes a child of from
    parentIds.clear();
    parentIds.add(fromId);
    CassandraUtils.executeCqlList(dbSource, (CassandraStatements) CassandraTest.cassandraItemDao.update(testId, toId, parentIds));

    VersionHistoryDag dag = CassandraTest.versionHistoryDagDao.retrieveFromDatabase(testId);

    assertEquals(2, dag.getEdgeIds().size());
    assertEquals(toId, (long) dag.getLeaves().get(0));

    VersionSuccessor fromSuccessor = CassandraTest.versionSuccessorDao.retrieveFromDatabase(
      dag.getEdgeIds().get(0));

    VersionSuccessor toSuccessor = CassandraTest.versionSuccessorDao.retrieveFromDatabase(
      dag.getEdgeIds().get(1));

    if (fromSuccessor.getFromId() != 0) {
      VersionSuccessor tmp = fromSuccessor;
      fromSuccessor = toSuccessor;
      toSuccessor = tmp;
    }

    assertEquals(0, fromSuccessor.getFromId());
    assertEquals(fromId, fromSuccessor.getToId());

    assertEquals(fromId, toSuccessor.getFromId());
    assertEquals(toId, toSuccessor.getToId());
  }

  @Test(expected = GroundException.class)
  public void testIncorrectUpdate() throws GroundException {
    long testId = 1;
    long fromId = 2;
    long toId = 3;

    try {
      CassandraTest.cassandraItemDao.create(new Item(testId, new HashMap<>()));

      CassandraUtils.executeCqlList(CassandraTest.dbSource, (CassandraStatements) CassandraTest.cassandraVersionDao.insert(new Version(toId)));
    } catch (GroundException ge) {
      fail(ge.getMessage());
    }

    List<Long> parentIds = new ArrayList<>();
    parentIds.add(fromId);

    // this should fail because fromId is not a valid version
    CassandraUtils.executeCqlList(dbSource, (CassandraStatements) CassandraTest.cassandraItemDao.update(testId, toId, parentIds));
  }

  @Test
  public void testMultipleParents() throws GroundException {
    long testId = 1;

    CassandraTest.cassandraItemDao.create(new Item(testId, new HashMap<>()));

    long parentOne = 2;
    long parentTwo = 3;
    long child = 4;

    CassandraUtils.executeCqlList(CassandraTest.dbSource, (CassandraStatements) CassandraTest.cassandraVersionDao.insert(new Version(parentOne)));
    CassandraUtils.executeCqlList(CassandraTest.dbSource, (CassandraStatements) CassandraTest.cassandraVersionDao.insert(new Version(parentTwo)));
    CassandraUtils.executeCqlList(CassandraTest.dbSource, (CassandraStatements) CassandraTest.cassandraVersionDao.insert(new Version(child)));
    List<Long> parentIds = new ArrayList<>();

    // first, make the parents children of EMPTY)
    CassandraUtils.executeCqlList(CassandraTest.dbSource, (CassandraStatements) CassandraTest.cassandraItemDao.update(testId, parentOne, parentIds));
    CassandraUtils.executeCqlList(CassandraTest.dbSource, (CassandraStatements) CassandraTest.cassandraItemDao.update(testId, parentTwo, parentIds));

    // then, add to as a child and make sure that it becomes a child of from
    parentIds.clear();
    parentIds.add(parentOne);
    parentIds.add(parentTwo);
    CassandraUtils.executeCqlList(dbSource, (CassandraStatements) CassandraTest.cassandraItemDao.update(testId, child, parentIds));

    VersionHistoryDag dag = CassandraTest.versionHistoryDagDao.retrieveFromDatabase(testId);

    assertEquals(4, dag.getEdgeIds().size());
    assertEquals(child, (long) dag.getLeaves().get(0));

    // Retrieve all the version successors and check that they have the correct data.
    VersionSuccessor parentOneSuccessor = CassandraTest.versionSuccessorDao
                                            .retrieveFromDatabase(
                                              dag.getEdgeIds().get(0));

    VersionSuccessor parentTwoSuccessor = CassandraTest.versionSuccessorDao
                                            .retrieveFromDatabase(
                                              dag.getEdgeIds().get(1));

    VersionSuccessor childOneSuccessor = CassandraTest.versionSuccessorDao.retrieveFromDatabase(
      dag.getEdgeIds().get(2));

    VersionSuccessor childTwoSuccessor = CassandraTest.versionSuccessorDao.retrieveFromDatabase(
      dag.getEdgeIds().get(3));

    assertEquals(0, parentOneSuccessor.getFromId());
    assertEquals(parentOne, parentOneSuccessor.getToId());

    assertEquals(0, parentTwoSuccessor.getFromId());
    assertEquals(parentTwo, parentTwoSuccessor.getToId());

    assertEquals(parentOne, childOneSuccessor.getFromId());
    assertEquals(child, childOneSuccessor.getToId());

    assertEquals(parentTwo, childTwoSuccessor.getFromId());
    assertEquals(child, childTwoSuccessor.getToId());
    assertEquals(child, childTwoSuccessor.getToId());
  }

  @Test
  public void testTags() throws GroundException {
    long testId = 1;
    Map<String, Tag> tags = new HashMap<>();
    tags.put("justkey", new Tag(testId, "justkey", null, null));
    tags.put("withintvalue", new Tag(testId, "withintvalue", 1, GroundType.INTEGER));
    tags.put("withstringvalue", new Tag(testId, "withstringvalue", "1", GroundType.STRING));
    tags.put("withboolvalue", new Tag(testId, "withboolvalue", true, GroundType.BOOLEAN));

    long itemId = CassandraTest.cassandraItemDao.create(new Item(testId, tags)).getId();

    Map<String, Tag> retrievedTags = CassandraTest.tagDao.retrieveFromDatabaseByItemId(itemId);

    assertEquals(tags.size(), retrievedTags.size());

    for (String key : tags.keySet()) {
      assert (retrievedTags).containsKey(key);
      assertEquals(tags.get(key), retrievedTags.get(key));
      assertEquals(itemId, retrievedTags.get(key).getId());
    }
  }
}
