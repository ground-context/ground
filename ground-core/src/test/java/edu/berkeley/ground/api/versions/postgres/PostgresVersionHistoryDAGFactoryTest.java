package edu.berkeley.ground.api.versions.postgres;

import org.junit.Test;

import edu.berkeley.ground.api.PostgresTest;
import edu.berkeley.ground.api.versions.VersionHistoryDAG;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class PostgresVersionHistoryDAGFactoryTest extends PostgresTest {

  public PostgresVersionHistoryDAGFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testVersionHistoryDAGCreation() throws GroundException {
    try {
      long testId = 1;
      super.versionHistoryDAGFactory.create(testId);

      VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(testId);

      assertEquals(0, dag.getEdgeIds().size());
    } finally {
      super.postgresClient.abort();
    }
  }
}
