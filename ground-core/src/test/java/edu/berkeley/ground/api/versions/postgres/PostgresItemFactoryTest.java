package edu.berkeley.ground.api.versions.postgres;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.berkeley.ground.api.PostgresTest;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.api.versions.Item;
import edu.berkeley.ground.api.versions.VersionHistoryDAG;
import edu.berkeley.ground.api.versions.VersionSuccessor;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class PostgresItemFactoryTest extends PostgresTest {

  public PostgresItemFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testCorrectUpdateWithParent() throws GroundException {
    try {
      long testId = 1;

      super.itemFactory.insertIntoDatabase(testId, new HashMap<>());

      long fromId = 2;
      long toId = 3;

      super.versionFactory.insertIntoDatabase(fromId);
      super.versionFactory.insertIntoDatabase(toId);

      List<Long> parentIds = new ArrayList<>();
      super.itemFactory.update(testId, fromId, parentIds);

      parentIds.clear();
      parentIds.add(fromId);
      super.itemFactory.update(testId, toId, parentIds);

      VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(testId);

      assertEquals(2, dag.getEdgeIds().size());
      assertEquals(toId, (long) dag.getLeaves().get(0));

      VersionSuccessor<?> successor = null;
      for (long id : dag.getEdgeIds()) {
        successor = super.versionSuccessorFactory.retrieveFromDatabase(id);

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
    } finally {
      super.postgresClient.abort();
    }
  }

  @Test
  public void testCorrectUpdateWithoutParent() throws GroundException {
    try {
      long testId = 1;

      super.itemFactory.insertIntoDatabase(testId, new HashMap<>());
      long toId = 2;
      super.versionFactory.insertIntoDatabase(toId);

      List<Long> parentIds = new ArrayList<>();

      // No parent is specified, and there is no other version in this Item, we should
      // automatically make this a child of EMPTY
      super.itemFactory.update(testId, toId, parentIds);

      VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(testId);

      assertEquals(1, dag.getEdgeIds().size());
      assertEquals(toId, (long) dag.getLeaves().get(0));

      VersionSuccessor<?> successor = super.versionSuccessorFactory.retrieveFromDatabase(
          dag.getEdgeIds().get(0));

      assertEquals(0, successor.getFromId());
      assertEquals(toId, successor.getToId());
    } finally {
      super.postgresClient.abort();
    }
  }

  @Test
  public void testCorrectUpdateWithLinearHistory() throws GroundException {
    try {
      long testId = 1;

      super.itemFactory.insertIntoDatabase(testId, new HashMap<>());

      long fromId = 2;
      long toId = 3;

      super.versionFactory.insertIntoDatabase(fromId);
      super.versionFactory.insertIntoDatabase(toId);
      List<Long> parentIds = new ArrayList<>();

      // first, make from a child of EMPTY
      super.itemFactory.update(testId, fromId, parentIds);

      // then, add to as a child and make sure that it becomes a child of from
      parentIds.clear();
      parentIds.add(fromId);
      super.itemFactory.update(testId, toId, parentIds);

      VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(testId);

      assertEquals(2, dag.getEdgeIds().size());
      assertEquals(toId, (long) dag.getLeaves().get(0));

      VersionSuccessor<?> fromSuccessor = super.versionSuccessorFactory.retrieveFromDatabase(
          dag.getEdgeIds().get(0));

      VersionSuccessor<?> toSuccessor = super.versionSuccessorFactory.retrieveFromDatabase(
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
      super.postgresClient.abort();
    }
  }

  @Test(expected = GroundException.class)
  public void testIncorrectUpdate() throws GroundException {
    try {
      long testId = 1;
      long fromId = 2;
      long toId = 3;

      try {
        super.itemFactory.insertIntoDatabase(testId, new HashMap<>());

        super.versionFactory.insertIntoDatabase(toId);
      } catch (GroundException ge) {
        fail(ge.getMessage());
      }

      List<Long> parentIds = new ArrayList<>();
      parentIds.add(fromId);

      // this should fail because fromId is not a valid version
      super.itemFactory.update(testId, toId, parentIds);
    } finally {
      super.postgresClient.abort();
    }
  }

  @Test
  public void testMultipleParents() throws GroundException {
    try {
      long testId = 1;

      super.itemFactory.insertIntoDatabase(testId, new HashMap<>());

      long parentOne = 2;
      long parentTwo = 3;
      long child = 4;

      super.versionFactory.insertIntoDatabase(parentOne);
      super.versionFactory.insertIntoDatabase(parentTwo);
      super.versionFactory.insertIntoDatabase(child);
      List<Long> parentIds = new ArrayList<>();

      // first, make the parents children of EMPTY
      super.itemFactory.update(testId, parentOne, parentIds);
      super.itemFactory.update(testId, parentTwo, parentIds);

      // then, add to as a child and make sure that it becomes a child of from
      parentIds.clear();
      parentIds.add(parentOne);
      parentIds.add(parentTwo);
      super.itemFactory.update(testId, child, parentIds);

      VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(testId);

      assertEquals(4, dag.getEdgeIds().size());
      assertEquals(child, (long) dag.getLeaves().get(0));

      // Retrieve all the version successors and check that they have the correct data.
      VersionSuccessor<?> parentOneSuccessor = super.versionSuccessorFactory.retrieveFromDatabase(
          dag.getEdgeIds().get(0));

      VersionSuccessor<?> parentTwoSuccessor = super.versionSuccessorFactory.retrieveFromDatabase(
          dag.getEdgeIds().get(1));

      VersionSuccessor<?> childOneSuccessor = super.versionSuccessorFactory.retrieveFromDatabase(
          dag.getEdgeIds().get(2));

      VersionSuccessor<?> childTwoSuccessor = super.versionSuccessorFactory.retrieveFromDatabase(
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
    } finally {
      super.postgresClient.abort();
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

      super.itemFactory.insertIntoDatabase(testId, tags);

      Item retrieved = super.itemFactory.retrieveFromDatabase(testId);

      assertEquals(testId, retrieved.getId());
      assertEquals(tags.size(), retrieved.getTags().size());

      Map<String, Tag> retrievedTags = retrieved.getTags();
      for (String key : tags.keySet()) {
        assert (retrievedTags).containsKey(key);
        assertEquals(tags.get(key), retrievedTags.get(key));
        assertEquals(retrieved.getId(), retrievedTags.get(key).getId());
      }
    } finally {
      super.postgresClient.abort();
    }
  }
}
