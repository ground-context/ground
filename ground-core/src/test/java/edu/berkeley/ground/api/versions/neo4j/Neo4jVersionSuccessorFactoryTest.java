package edu.berkeley.ground.api.versions.neo4j;

import org.junit.Test;

import edu.berkeley.ground.api.Neo4jTest;
import edu.berkeley.ground.api.versions.VersionSuccessor;
import edu.berkeley.ground.db.Neo4jClient.Neo4jConnection;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class Neo4jVersionSuccessorFactoryTest extends Neo4jTest {

  public Neo4jVersionSuccessorFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testVersionSuccessorCreation() throws GroundException {
    Neo4jConnection connection = null;
    try {
      connection = super.neo4jClient.getConnection();
      long fromNodeId = super.factories.getNodeFactory().create("testFromNode").getId();
      long toNodeId = super.factories.getNodeFactory().create("testToNode").getId();
      long fromId = super.createNodeVersion(fromNodeId);
      long toId = super.createNodeVersion(toNodeId);

      long vsId = super.versionSuccessorFactory.create(connection, fromId, toId).getId();

      VersionSuccessor<?> retrieved = super.versionSuccessorFactory.retrieveFromDatabase(
          connection, vsId);

      assertEquals(fromId, retrieved.getFromId());
      assertEquals(toId, retrieved.getToId());
    } catch (Exception e) {
      fail(e.getMessage());
    } finally {
      connection.abort();
    }
  }

  @Test(expected = GroundException.class)
  public void testBadVersionSuccessorCreation() throws GroundException {
    Neo4jConnection connection = null;
    try {
      long toId = -1;

      try {
        String nodeName = "testNode";
        long nodeId = super.factories.getNodeFactory().create(nodeName).getId();
        connection = super.neo4jClient.getConnection();
        toId = super.createNodeVersion(nodeId);
      } catch (GroundException ge) {
        fail(ge.getMessage());
      }

      // this statement should be fail because the fromId does not exist
      super.versionSuccessorFactory.create(connection, 9, toId).getId();
    } finally {
      connection.abort();
    }
  }
}
