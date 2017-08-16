package edu.berkeley.ground.cassandra.dao.usage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.usage.LineageGraph;
import edu.berkeley.ground.common.model.version.VersionHistoryDag;
import edu.berkeley.ground.common.model.version.VersionSuccessor;
import edu.berkeley.ground.cassandra.dao.CassandraTest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.junit.Test;

public class CassandraLineageGraphDaoTest extends CassandraTest {

  public CassandraLineageGraphDaoTest() throws GroundException {
    super();
  }

  @Test
  public void testLineageGraphCreation() throws GroundException {
    String testName = "test";
    String sourceKey = "testKey";

    LineageGraph lineageGraph = new LineageGraph(0L, testName, sourceKey, new HashMap<>());
    CassandraTest.lineageGraphDao.create(lineageGraph);
    LineageGraph graph = CassandraTest.lineageGraphDao.retrieveFromDatabase(sourceKey);

    assertEquals(testName, graph.getName());
    assertEquals(sourceKey, graph.getSourceKey());
  }

  @Test(expected = GroundException.class)
  public void testRetrieveBadLineageGraph() throws GroundException {
    String sourceKey = "test";

    try {
      CassandraTest.lineageGraphDao.retrieveFromDatabase(sourceKey);
    } catch (GroundException e) {
      assertEquals(GroundException.class, e.getClass());

      throw e;
    }
  }

  @Test(expected = GroundException.class)
  public void testCreateDuplicateLineageGraph() throws GroundException {
    String lineageGraphName = "lineageGraphName";
    String lineageGraphKey = "lineageGraphKey";

    try {
      LineageGraph lineageGraph = new LineageGraph(0L, lineageGraphName, lineageGraphKey,
                                                    new HashMap<>());
      CassandraTest.lineageGraphDao.create(lineageGraph);
    } catch (GroundException e) {
      fail(e.getMessage());
    }

    LineageGraph duplicateGraph = new LineageGraph(0L, lineageGraphName, lineageGraphKey,
                                                    new HashMap<>());
    CassandraTest.lineageGraphDao.create(duplicateGraph);
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
    long lineageEdgeVersionId = CassandraTest.createLineageEdgeVersion(lineageEdgeId, firstNodeVersionId, secondNodeVersionId).getId();

    List<Long> lineageEdgeVersionIds = new ArrayList<>();
    lineageEdgeVersionIds.add(lineageEdgeVersionId);

    String lineageGraphName = "testLineageGraph";
    long lineageGraphId = CassandraTest.createLineageGraph(lineageGraphName).getId();
    long lineageGraphVersionId = CassandraTest.createLineageGraphVersion(lineageGraphId, lineageEdgeVersionIds).getId();

    List<Long> parents = new ArrayList<>();
    parents.add(lineageGraphVersionId);
    long newLineageGraphVersionId = CassandraTest.createLineageGraphVersion(lineageGraphId, lineageEdgeVersionIds, parents).getId();

    CassandraTest.lineageGraphDao.truncate(lineageGraphId, 1);

    VersionHistoryDag dag = CassandraTest.versionHistoryDagDao
                              .retrieveFromDatabase(lineageGraphId);

    assertEquals(1, dag.getEdgeIds().size());

    VersionSuccessor successor = CassandraTest.versionSuccessorDao.retrieveFromDatabase(
      dag.getEdgeIds().get(0));

    assertEquals(0, successor.getFromId());
    assertEquals(newLineageGraphVersionId, successor.getToId());
  }
}
