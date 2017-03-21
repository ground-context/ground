package edu.berkeley.ground.api.versions.neo4j;

import org.junit.Test;

import edu.berkeley.ground.api.Neo4jTest;
import edu.berkeley.ground.api.versions.VersionHistoryDAG;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class Neo4jVersionHistoryDAGFactoryTest extends Neo4jTest {

  public Neo4jVersionHistoryDAGFactoryTest() throws GroundException {
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
      super.neo4jClient.abort();
    }
  }
}
