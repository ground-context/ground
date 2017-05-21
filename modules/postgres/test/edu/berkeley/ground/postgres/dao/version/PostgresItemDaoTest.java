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

package edu.berkeley.ground.postgres.dao.version;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.version.GroundType;
import edu.berkeley.ground.common.model.version.Item;
import edu.berkeley.ground.common.model.version.Tag;
import edu.berkeley.ground.common.model.version.Version;
import edu.berkeley.ground.common.model.version.VersionHistoryDag;
import edu.berkeley.ground.common.model.version.VersionSuccessor;
import edu.berkeley.ground.postgres.dao.PostgresTest;
import edu.berkeley.ground.postgres.util.PostgresStatements;
import edu.berkeley.ground.postgres.util.PostgresUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class PostgresItemDaoTest extends PostgresTest {

  public PostgresItemDaoTest() throws GroundException {
    super();
  }

  @Test
  public void testCorrectUpdateWithParent() throws GroundException {
    long testId = 1;

    Item item = new Item(testId, new HashMap<>());
    PostgresTest.postgresItemDao.create(item);

    long fromId = 2;
    long toId = 3;

    PostgresUtils.executeSqlList(PostgresTest.dbSource, (PostgresStatements) PostgresTest.postgresVersionDao.insert(new Version(fromId)));
    PostgresUtils.executeSqlList(PostgresTest.dbSource, (PostgresStatements) PostgresTest.postgresVersionDao.insert(new Version(toId)));

    List<Long> parentIds = new ArrayList<>();
    PostgresUtils.executeSqlList(dbSource, (PostgresStatements) PostgresTest.postgresItemDao.update(testId, fromId, parentIds));

    parentIds.clear();
    parentIds.add(fromId);
    PostgresUtils.executeSqlList(dbSource, (PostgresStatements) PostgresTest.postgresItemDao.update(testId, toId, parentIds));

    VersionHistoryDag dag = PostgresTest.versionHistoryDagDao.retrieveFromDatabase(testId);

    assertEquals(2, dag.getEdgeIds().size());
    assertEquals(toId, (long) dag.getLeaves().get(0));

    VersionSuccessor successor = null;
    for (long id : dag.getEdgeIds()) {
      successor = PostgresTest.versionSuccessorDao.retrieveFromDatabase(id);

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

    PostgresTest.postgresItemDao.create(new Item(testId, new HashMap<>()));
    long toId = 2;
    PostgresUtils.executeSqlList(PostgresTest.dbSource, (PostgresStatements) PostgresTest.postgresVersionDao.insert(new Version(toId)));

    List<Long> parentIds = new ArrayList<>();

    // No parent is specified, and there is no other version in this Item, we should
    // automatically make this a child of EMPTY
    PostgresUtils.executeSqlList(dbSource, (PostgresStatements) PostgresTest.postgresItemDao.update(testId, toId, parentIds));

    VersionHistoryDag dag = PostgresTest.versionHistoryDagDao.retrieveFromDatabase(testId);

    assertEquals(1, dag.getEdgeIds().size());
    assertEquals(toId, (long) dag.getLeaves().get(0));

    VersionSuccessor successor = PostgresTest.versionSuccessorDao.retrieveFromDatabase(
      dag.getEdgeIds().get(0));

    assertEquals(0, successor.getFromId());
    assertEquals(toId, successor.getToId());
  }

  @Test
  public void testCorrectUpdateWithLinearHistory() throws GroundException {
    long testId = 1;

    PostgresTest.postgresItemDao.create(new Item(testId, new HashMap<>()));

    long fromId = 2;
    long toId = 3;

    PostgresUtils.executeSqlList(PostgresTest.dbSource, (PostgresStatements) PostgresTest.postgresVersionDao.insert(new Version(fromId)));
    PostgresUtils.executeSqlList(PostgresTest.dbSource, (PostgresStatements) PostgresTest.postgresVersionDao.insert(new Version(toId)));
    List<Long> parentIds = new ArrayList<>();

    // first, make from a child of EMPTY
    PostgresUtils.executeSqlList(dbSource, (PostgresStatements) PostgresTest.postgresItemDao.update(testId, fromId, parentIds));

    // then, add to as a child and make sure that it becomes a child of from
    parentIds.clear();
    parentIds.add(fromId);
    PostgresUtils.executeSqlList(dbSource, (PostgresStatements) PostgresTest.postgresItemDao.update(testId, toId, parentIds));

    VersionHistoryDag dag = PostgresTest.versionHistoryDagDao.retrieveFromDatabase(testId);

    assertEquals(2, dag.getEdgeIds().size());
    assertEquals(toId, (long) dag.getLeaves().get(0));

    VersionSuccessor fromSuccessor = PostgresTest.versionSuccessorDao.retrieveFromDatabase(
      dag.getEdgeIds().get(0));

    VersionSuccessor toSuccessor = PostgresTest.versionSuccessorDao.retrieveFromDatabase(
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
      PostgresTest.postgresItemDao.create(new Item(testId, new HashMap<>()));

      PostgresUtils.executeSqlList(PostgresTest.dbSource, (PostgresStatements) PostgresTest.postgresVersionDao.insert(new Version(toId)));
    } catch (GroundException ge) {
      fail(ge.getMessage());
    }

    List<Long> parentIds = new ArrayList<>();
    parentIds.add(fromId);

    // this should fail because fromId is not a valid version
    PostgresUtils.executeSqlList(dbSource, (PostgresStatements) PostgresTest.postgresItemDao.update(testId, toId, parentIds));
  }

  @Test
  public void testMultipleParents() throws GroundException {
    long testId = 1;

    PostgresTest.postgresItemDao.create(new Item(testId, new HashMap<>()));

    long parentOne = 2;
    long parentTwo = 3;
    long child = 4;

    PostgresUtils.executeSqlList(PostgresTest.dbSource, (PostgresStatements) PostgresTest.postgresVersionDao.insert(new Version(parentOne)));
    PostgresUtils.executeSqlList(PostgresTest.dbSource, (PostgresStatements) PostgresTest.postgresVersionDao.insert(new Version(parentTwo)));
    PostgresUtils.executeSqlList(PostgresTest.dbSource, (PostgresStatements) PostgresTest.postgresVersionDao.insert(new Version(child)));
    List<Long> parentIds = new ArrayList<>();

    // first, make the parents children of EMPTY)
    PostgresUtils.executeSqlList(PostgresTest.dbSource, (PostgresStatements) PostgresTest.postgresItemDao.update(testId, parentOne, parentIds));
    PostgresUtils.executeSqlList(PostgresTest.dbSource, (PostgresStatements) PostgresTest.postgresItemDao.update(testId, parentTwo, parentIds));

    // then, add to as a child and make sure that it becomes a child of from
    parentIds.clear();
    parentIds.add(parentOne);
    parentIds.add(parentTwo);
    PostgresUtils.executeSqlList(dbSource, (PostgresStatements) PostgresTest.postgresItemDao.update(testId, child, parentIds));

    VersionHistoryDag dag = PostgresTest.versionHistoryDagDao.retrieveFromDatabase(testId);

    assertEquals(4, dag.getEdgeIds().size());
    assertEquals(child, (long) dag.getLeaves().get(0));

    // Retrieve all the version successors and check that they have the correct data.
    VersionSuccessor parentOneSuccessor = PostgresTest.versionSuccessorDao
                                            .retrieveFromDatabase(
                                              dag.getEdgeIds().get(0));

    VersionSuccessor parentTwoSuccessor = PostgresTest.versionSuccessorDao
                                            .retrieveFromDatabase(
                                              dag.getEdgeIds().get(1));

    VersionSuccessor childOneSuccessor = PostgresTest.versionSuccessorDao.retrieveFromDatabase(
      dag.getEdgeIds().get(2));

    VersionSuccessor childTwoSuccessor = PostgresTest.versionSuccessorDao.retrieveFromDatabase(
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

    long itemId = PostgresTest.postgresItemDao.create(new Item(testId, tags)).getId();

    Map<String, Tag> retrievedTags = PostgresTest.tagDao.retrieveFromDatabaseByItemId(itemId);

    assertEquals(tags.size(), retrievedTags.size());

    for (String key : tags.keySet()) {
      assert (retrievedTags).containsKey(key);
      assertEquals(tags.get(key), retrievedTags.get(key));
      assertEquals(itemId, retrievedTags.get(key).getId());
    }
  }
}
