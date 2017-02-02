package edu.berkeley.ground.api.versions.gremlin;

import org.junit.Test;

import edu.berkeley.ground.api.GremlinTest;
import edu.berkeley.ground.api.versions.VersionHistoryDAG;
import edu.berkeley.ground.api.versions.VersionSuccessor;
import edu.berkeley.ground.db.GremlinClient.GremlinConnection;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class GremlinVersionHistoryFactoryDAGTest extends GremlinTest {

  public GremlinVersionHistoryFactoryDAGTest() throws GroundException {
    super();
  }

  @Test
  public void testVersionHistoryDAGCreation() throws GroundException {
    GremlinConnection connection = null;
    try {
      String testId = super.factories.getNodeFactory().create("test").getId();
      super.versionHistoryDAGFactory.create(testId);
      connection = super.gremlinClient.getConnection();

      VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(connection,
          testId);

      assertEquals(0, dag.getEdgeIds().size());

      connection.abort();
    } finally {
      connection.abort();
    }
  }
}
