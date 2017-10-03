package edu.berkeley.ground.cassandra.dao.usage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.usage.LineageEdge;
import edu.berkeley.ground.common.model.version.VersionHistoryDag;
import edu.berkeley.ground.common.model.version.VersionSuccessor;
import edu.berkeley.ground.cassandra.dao.CassandraTest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.junit.Test;

public class CassandraLineageEdgeDaoTest extends CassandraTest {

  public CassandraLineageEdgeDaoTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageEdgeCreation() throws GroundException {
    String testName = "test";
    String sourceKey = "testKey";

    LineageEdge insertLineageEdge = new LineageEdge(0L, testName, sourceKey, new HashMap<>());
    CassandraTest.lineageEdgeDao.create(insertLineageEdge);
    LineageEdge lineageEdge = CassandraTest.lineageEdgeDao.retrieveFromDatabase(sourceKey);

    assertEquals(testName, lineageEdge.getName());
    assertEquals(sourceKey, lineageEdge.getSourceKey());
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadLineageEdge() throws GroundException {
    String sourceKey = "test";

    try {
      CassandraTest.lineageEdgeDao.retrieveFromDatabase(sourceKey);
    } catch (GroundException e) {
      assertEquals(GroundException.class, e.getClass());

      throw e;
    }
  }

  @Test(expected = GroundException.class)
  public void testCreateDuplicateLineageEdge() throws GroundException {
    String lineageEdgeName = "lineageEdgeName";
    String lineageEdgeKey = "lineageEdgeKey";

    try {
      LineageEdge lineageEdge = new LineageEdge(0L, lineageEdgeName, lineageEdgeKey,
                                                 new HashMap<>());
      CassandraTest.lineageEdgeDao.create(lineageEdge);
    } catch (GroundException e) {
      fail(e.getMessage());
    }

    LineageEdge duplicateEdge = new LineageEdge(0L, lineageEdgeName, lineageEdgeKey,
                                                 new HashMap<>());
    CassandraTest.lineageEdgeDao.create(duplicateEdge);
  }

  @Test
  public void testTruncate() throws GroundException {
    String firstTestNode = "firstTestNode";
    long firstTestNodeId = CassandraTest.createNode(firstTestNode).getId();
    long firstNodeVersionId = CassandraTest.createNodeVersion(firstTestNodeId).getId();

    String secondTestNode = "secondTestNode";
    long secondTestNodeId = CassandraTest.createNode(secondTestNode).getId();
    long secondNodeVersionId = CassandraTest.createNodeVersion(secondTestNodeId).getId();

    String lineageEdgeName = "testLineageEdge";
    long lineageEdgeId = CassandraTest.createLineageEdge(lineageEdgeName).getId();
    long lineageEdgeVersionId = CassandraTest.createLineageEdgeVersion(lineageEdgeId,
      firstNodeVersionId, secondNodeVersionId).getId();

    List<Long> parents = new ArrayList<>();
    parents.add(lineageEdgeVersionId);
    long newLineageEdgeVersionId = CassandraTest.createLineageEdgeVersion(lineageEdgeId,
      firstNodeVersionId, secondNodeVersionId, parents).getId();

    CassandraTest.lineageEdgeDao.truncate(lineageEdgeId, 1);

    VersionHistoryDag dag = CassandraTest.versionHistoryDagDao
                              .retrieveFromDatabase(lineageEdgeId);

    assertEquals(1, dag.getEdgeIds().size());

    VersionSuccessor successor = CassandraTest.versionSuccessorDao.retrieveFromDatabase(
      dag.getEdgeIds().get(0));

    assertEquals(0, successor.getFromId());
    assertEquals(newLineageEdgeVersionId, successor.getToId());
  }
}
