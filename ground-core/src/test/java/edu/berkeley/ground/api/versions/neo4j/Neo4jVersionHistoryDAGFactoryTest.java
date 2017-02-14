package edu.berkeley.ground.api.versions.neo4j;

import org.junit.Test;

import edu.berkeley.ground.api.Neo4jTest;
import edu.berkeley.ground.api.versions.VersionHistoryDAG;
import edu.berkeley.ground.db.Neo4jClient.Neo4jConnection;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class Neo4jVersionHistoryDAGFactoryTest extends Neo4jTest {

  public Neo4jVersionHistoryDAGFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testVersionHistoryDAGCreation() throws GroundException {
    Neo4jConnection connection = null;
    try {
      long testId = 1;
      super.versionHistoryDAGFactory.create(testId);
      connection = super.neo4jClient.getConnection();

      VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(connection, testId);

      assertEquals(0, dag.getEdgeIds().size());
    } finally {
      connection.abort();
    }
  }
}
