package edu.berkeley.ground.api.versions.cassandra;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.berkeley.ground.api.CassandraTest;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.api.versions.Item;
import edu.berkeley.ground.api.versions.VersionHistoryDAG;
import edu.berkeley.ground.api.versions.VersionSuccessor;
import edu.berkeley.ground.db.CassandraClient.CassandraConnection;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class CassandraItemFactoryTest extends CassandraTest {
  /* Note that there is no creation test here because there's no need to ever explicitly
  * retrieve an Item. */

  public CassandraItemFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testCorrectUpdateWithParent() throws GroundException {
    CassandraConnection connection = null;
    try {
      long testId = 1;
      connection = super.cassandraClient.getConnection();

      super.itemFactory.insertIntoDatabase(connection, testId, new HashMap<>());

      long fromId = 123;
      long toId = 456;

      super.versionFactory.insertIntoDatabase(connection, fromId);
      super.versionFactory.insertIntoDatabase(connection, toId);

      List<Long> parentIds = new ArrayList<>();
      parentIds.add(0L);
      super.itemFactory.update(connection, testId, fromId, new ArrayList<>());

      parentIds.clear();
      parentIds.add(fromId);
      super.itemFactory.update(connection, testId, toId, parentIds);

      VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(connection,
          testId);

      assertEquals(2, dag.getEdgeIds().size());
      assertEquals(toId, (long) dag.getLeaves().get(0));

      VersionSuccessor<?> successor = null;
      for (long id : dag.getEdgeIds()) {
        successor = super.versionSuccessorFactory.retrieveFromDatabase(connection, id);

        if (!(successor.getFromId() == 0)) {
          break;
        }
      }

      if (successor == null) {
        fail();
      }

      assertEquals(fromId, successor.getFromId());
      assertEquals(toId, successor.getToId());
    } finally {
      connection.abort();
    }
  }

  @Test
  public void testCorrectUpdateWithoutParent() throws GroundException {
    CassandraConnection connection = null;
    try {
      long testId = 1;
      connection = super.cassandraClient.getConnection();

      super.itemFactory.insertIntoDatabase(connection, testId, new HashMap<>());
      long toId = 123;
      super.versionFactory.insertIntoDatabase(connection, toId);

      List<Long> parentIds = new ArrayList<>();

      // No parent is specified, and there is no other version in this Item, we should
      // automatically make this a child of EMPTY
      super.itemFactory.update(connection, testId, toId, parentIds);

      VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(connection,
          testId);

      assertEquals(1, dag.getEdgeIds().size());
      assertEquals(toId, (long) dag.getLeaves().get(0));

      VersionSuccessor<?> successor = super.versionSuccessorFactory.retrieveFromDatabase(
          connection, dag.getEdgeIds().get(0));

      assertEquals(0, successor.getFromId());
      assertEquals(toId, successor.getToId());
    } finally {
      connection.abort();
    }
  }

  @Test
  public void testCorrectUpdateWithLinearHistory() throws GroundException {
    CassandraConnection connection = null;

    try {
      long testId = 1;
      connection = super.cassandraClient.getConnection();

      super.itemFactory.insertIntoDatabase(connection, testId, new HashMap<>());

      long fromId = 123;
      long toId = 456;

      super.versionFactory.insertIntoDatabase(connection, fromId);
      super.versionFactory.insertIntoDatabase(connection, toId);
      List<Long> parentIds = new ArrayList<>();

      // first, make from a child of EMPTY
      super.itemFactory.update(connection, testId, fromId, parentIds);


      // then, add to as a child and make sure that it becomes a child of from
      parentIds = new ArrayList<>();
      parentIds.add(fromId);
      super.itemFactory.update(connection, testId, toId, parentIds);

      VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(connection,
          testId);

      assertEquals(2, dag.getEdgeIds().size());
      assertEquals(toId, (long) dag.getLeaves().get(0));

      VersionSuccessor<?> toSuccessor = super.versionSuccessorFactory.retrieveFromDatabase(
          connection, dag.getEdgeIds().get(0));

      VersionSuccessor<?> fromSuccessor = super.versionSuccessorFactory.retrieveFromDatabase(
          connection, dag.getEdgeIds().get(1));

      if (!(fromSuccessor.getFromId() == 0)) {
        VersionSuccessor<?> tmp = fromSuccessor;
        fromSuccessor = toSuccessor;
        toSuccessor = tmp;
      }

      assertEquals(0, fromSuccessor.getFromId());
      assertEquals(fromId, fromSuccessor.getToId());

      assertEquals(fromId, toSuccessor.getFromId());
      assertEquals(toId, toSuccessor.getToId());
    } finally {
      connection.abort();
    }
  }

  @Test(expected = GroundException.class)
  public void testIncorrectUpdate() throws GroundException {
    CassandraConnection connection = null;
    try {
      long testId = 1;
      long fromId = 123;
      long toId = 456;

      try {
        connection = super.cassandraClient.getConnection();

        super.itemFactory.insertIntoDatabase(connection, testId, new HashMap<>());

        super.versionFactory.insertIntoDatabase(connection, toId);

      } catch (GroundException ge) {
        fail(ge.getMessage());
      }

      List<Long> parentIds = new ArrayList<>();
      parentIds.add(fromId);

      // this should fail because fromId is not a valid version
      super.itemFactory.update(connection, testId, toId, parentIds);
    } finally {
      connection.abort();
    }
  }

  @Test
  public void testMultipleParents() throws GroundException {
    CassandraConnection connection = null;
    try {
      long testId = 1;
      connection = super.cassandraClient.getConnection();

      super.itemFactory.insertIntoDatabase(connection, testId, new HashMap<>());

      long parentOne = 123;
      long parentTwo = 456;
      long child = 789;

      super.versionFactory.insertIntoDatabase(connection, parentOne);
      super.versionFactory.insertIntoDatabase(connection, parentTwo);
      super.versionFactory.insertIntoDatabase(connection, child);
      List<Long> parentIds = new ArrayList<>();

      // first, make the parents children of EMPTY
      super.itemFactory.update(connection, testId, parentOne, parentIds);
      super.itemFactory.update(connection, testId, parentTwo, parentIds);

      // then, add to as a child and make sure that it becomes a child of from
      parentIds = new ArrayList<>();
      parentIds.add(parentOne);
      parentIds.add(parentTwo);
      super.itemFactory.update(connection, testId, child, parentIds);

      VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(connection,
          testId);

      assertEquals(4, dag.getEdgeIds().size());
      assertEquals(1, dag.getLeaves().size());
      assertEquals(child, (long) dag.getLeaves().get(0));

      // No need to check the version successors because we have tests for those.
    } finally {
      connection.abort();
    }
  }

  @Test
  public void testTags() throws GroundException {
    CassandraConnection connection = null;

    try {
      long testId = 1;
      connection = super.cassandraClient.getConnection();
      Map<String, Tag> tags = new HashMap<>();
      tags.put("justkey", new Tag(-1, "justkey", null, null));
      tags.put("withintvalue", new Tag(-1, "withintvalue", 1, GroundType.INTEGER));
      tags.put("withstringvalue", new Tag(-1, "withstringvalue", "1", GroundType.STRING));
      tags.put("withboolvalue", new Tag(-1, "withboolvalue", true, GroundType.BOOLEAN));


      super.itemFactory.insertIntoDatabase(connection, testId, tags);

      Item retrieved = super.itemFactory.retrieveFromDatabase(connection, testId);

      assertEquals(testId, retrieved.getId());
      assertEquals(tags.size(), retrieved.getTags().size());

      Map<String, Tag> retrievedTags = retrieved.getTags();
      for (String key : tags.keySet()) {
        assert (retrievedTags).containsKey(key);
        assertEquals(tags.get(key), retrievedTags.get(key));
        assertEquals(retrieved.getId(), retrievedTags.get(key).getId());
      }
    } finally {
      connection.abort();
    }
  }
}
