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

package edu.berkeley.ground.dao.versions.cassandra;

import org.junit.Test;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.berkeley.ground.dao.CassandraTest;
import edu.berkeley.ground.dao.models.cassandra.CassandraTagFactory;
import edu.berkeley.ground.dao.versions.cassandra.mock.TestCassandraItemFactory;
import edu.berkeley.ground.dao.versions.cassandra.mock.TestCassandraVersionFactory;
import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.exceptions.GroundDbException;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.model.versions.Item;
import edu.berkeley.ground.model.versions.Version;
import edu.berkeley.ground.model.versions.VersionHistoryDag;
import edu.berkeley.ground.model.versions.VersionSuccessor;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class CassandraItemFactoryTest extends CassandraTest {
  /* Note that there is no creation test here because there's no need to ever explicitly
  * retrieve an Item. */

  private TestCassandraItemFactory itemFactory;
  private TestCassandraVersionFactory versionFactory;


  public CassandraItemFactoryTest() throws GroundException {
    super();

    this.itemFactory = new TestCassandraItemFactory(CassandraTest.cassandraClient,
        CassandraTest.versionHistoryDAGFactory, CassandraTest.tagFactory);

    this.versionFactory = new TestCassandraVersionFactory(CassandraTest.cassandraClient);
  }

  @Test
  public void testCorrectUpdateWithParent() throws GroundException {
    try {
      long testId = 1;

      this.itemFactory.insertIntoDatabase(testId, new HashMap<>());

      long fromId = 123;
      long toId = 456;

      this.versionFactory.insertIntoDatabase(fromId);
      this.versionFactory.insertIntoDatabase(toId);

      List<Long> parentIds = new ArrayList<>();
      parentIds.add(0L);
      this.itemFactory.update(testId, fromId, new ArrayList<>());

      parentIds.clear();
      parentIds.add(fromId);
      this.itemFactory.update(testId, toId, parentIds);

      VersionHistoryDag<?> dag = CassandraTest.versionHistoryDAGFactory.retrieveFromDatabase(testId);

      assertEquals(2, dag.getEdgeIds().size());
      assertEquals(toId, (long) dag.getLeaves().get(0));

      VersionSuccessor<?> successor = null;
      for (long id : dag.getEdgeIds()) {
        successor = CassandraTest.versionSuccessorFactory.retrieveFromDatabase(id);

        if (successor.getFromId() != 0) {
          break;
        }
      }

      if (successor == null) {
        fail();
      }

      assertEquals(fromId, successor.getFromId());
      assertEquals(toId, successor.getToId());
    } finally {
      CassandraTest.cassandraClient.abort();
    }
  }

  @Test
  public void testCorrectUpdateWithoutParent() throws GroundException {
    try {
      long testId = 1;

      this.itemFactory.insertIntoDatabase(testId, new HashMap<>());
      long toId = 123;
      this.versionFactory.insertIntoDatabase(toId);

      List<Long> parentIds = new ArrayList<>();

      // No parent is specified, and there is no other version in this Item, we should
      // automatically make this a child of EMPTY
      this.itemFactory.update(testId, toId, parentIds);

      VersionHistoryDag<?> dag = CassandraTest.versionHistoryDAGFactory.retrieveFromDatabase(testId);

      assertEquals(1, dag.getEdgeIds().size());
      assertEquals(toId, (long) dag.getLeaves().get(0));

      VersionSuccessor<?> successor = CassandraTest.versionSuccessorFactory.retrieveFromDatabase(
          dag.getEdgeIds().get(0));

      assertEquals(0, successor.getFromId());
      assertEquals(toId, successor.getToId());
    } finally {
      CassandraTest.cassandraClient.abort();
    }
  }

  @Test
  public void testCorrectUpdateWithLinearHistory() throws GroundException {
    try {
      long testId = 1;

      this.itemFactory.insertIntoDatabase(testId, new HashMap<>());

      long fromId = 123;
      long toId = 456;

      this.versionFactory.insertIntoDatabase(fromId);
      this.versionFactory.insertIntoDatabase(toId);
      List<Long> parentIds = new ArrayList<>();

      // first, make from a child of EMPTY
      this.itemFactory.update(testId, fromId, parentIds);

      // then, add to as a child and make sure that it becomes a child of from
      parentIds = new ArrayList<>();
      parentIds.add(fromId);
      this.itemFactory.update(testId, toId, parentIds);

      VersionHistoryDag<?> dag = CassandraTest.versionHistoryDAGFactory.retrieveFromDatabase(testId);

      assertEquals(2, dag.getEdgeIds().size());
      assertEquals(toId, (long) dag.getLeaves().get(0));

      VersionSuccessor<?> toSuccessor = CassandraTest.versionSuccessorFactory.retrieveFromDatabase(
          dag.getEdgeIds().get(0));

      VersionSuccessor<?> fromSuccessor = CassandraTest.versionSuccessorFactory.retrieveFromDatabase(
          dag.getEdgeIds().get(1));

      if (fromSuccessor.getFromId() != 0) {
        VersionSuccessor<?> tmp = fromSuccessor;
        fromSuccessor = toSuccessor;
        toSuccessor = tmp;
      }

      assertEquals(0, fromSuccessor.getFromId());
      assertEquals(fromId, fromSuccessor.getToId());

      assertEquals(fromId, toSuccessor.getFromId());
      assertEquals(toId, toSuccessor.getToId());
    } finally {
      CassandraTest.cassandraClient.abort();
    }
  }

  @Test(expected = GroundException.class)
  public void testIncorrectUpdate() throws GroundException {
    try {
      long testId = 1;
      long fromId = 123;
      long toId = 456;

      try {
        this.itemFactory.insertIntoDatabase(testId, new HashMap<>());

        this.versionFactory.insertIntoDatabase(toId);
      } catch (GroundException ge) {
        fail(ge.getMessage());
      }

      List<Long> parentIds = new ArrayList<>();
      parentIds.add(fromId);

      // this should fail because fromId is not a valid version
      this.itemFactory.update(testId, toId, parentIds);
    } finally {
      CassandraTest.cassandraClient.abort();
    }
  }

  @Test
  public void testMultipleParents() throws GroundException {
    try {
      long testId = 1;

      this.itemFactory.insertIntoDatabase(testId, new HashMap<>());

      long parentOne = 123;
      long parentTwo = 456;
      long child = 789;

      this.versionFactory.insertIntoDatabase(parentOne);
      this.versionFactory.insertIntoDatabase(parentTwo);
      this.versionFactory.insertIntoDatabase(child);
      List<Long> parentIds = new ArrayList<>();

      // first, make the parents children of EMPTY
      this.itemFactory.update(testId, parentOne, parentIds);
      this.itemFactory.update(testId, parentTwo, parentIds);

      // then, add to as a child and make sure that it becomes a child of from
      parentIds = new ArrayList<>();
      parentIds.add(parentOne);
      parentIds.add(parentTwo);
      this.itemFactory.update(testId, child, parentIds);

      VersionHistoryDag<?> dag = CassandraTest.versionHistoryDAGFactory.retrieveFromDatabase(testId);

      assertEquals(4, dag.getEdgeIds().size());
      assertEquals(1, dag.getLeaves().size());
      assertEquals(child, (long) dag.getLeaves().get(0));

      // No need to check the version successors because we have tests for those.
    } finally {
      CassandraTest.cassandraClient.abort();
    }
  }

  @Test
  public void testTags() throws GroundException {
    try {
      long testId = 1;
      Map<String, Tag> tags = new HashMap<>();
      tags.put("justkey", new Tag(-1, "justkey", null, null));
      tags.put("withintvalue", new Tag(-1, "withintvalue", 1, GroundType.INTEGER));
      tags.put("withstringvalue", new Tag(-1, "withstringvalue", "1", GroundType.STRING));
      tags.put("withboolvalue", new Tag(-1, "withboolvalue", true, GroundType.BOOLEAN));

      this.itemFactory.insertIntoDatabase(testId, tags);

      Item retrieved = this.itemFactory.retrieveFromDatabase(testId);

      assertEquals(testId, retrieved.getId());
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
}
