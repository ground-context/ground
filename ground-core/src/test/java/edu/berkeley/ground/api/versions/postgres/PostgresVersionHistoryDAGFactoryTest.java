package edu.berkeley.ground.api.versions.postgres;

import org.junit.Test;

import edu.berkeley.ground.api.PostgresTest;
import edu.berkeley.ground.api.versions.VersionHistoryDAG;
import edu.berkeley.ground.db.PostgresClient.PostgresConnection;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class PostgresVersionHistoryDAGFactoryTest extends PostgresTest {

  public PostgresVersionHistoryDAGFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testVersionHistoryDAGCreation() throws GroundException {
    PostgresConnection connection = null;
    try {
      long testId = 1;
      super.versionHistoryDAGFactory.create(testId);
      connection = super.cassandraClient.getConnection();

      VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(connection,
          testId);

      assertEquals(0, dag.getEdgeIds().size());
    } finally {
      connection.abort();
    }
  }
}
