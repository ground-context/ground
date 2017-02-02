package edu.berkeley.ground.api.versions.gremlin;

import org.junit.Test;

import edu.berkeley.ground.api.GremlinTest;
import edu.berkeley.ground.api.versions.VersionSuccessor;
import edu.berkeley.ground.db.GremlinClient.GremlinConnection;
import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class GremlinVersionSuccessorFactoryTest extends GremlinTest {
    /* Note: In Gremlin, we don't create explicit Versions beacuse all of the logic is wrapped in
     * FooVersions. We are using NodeVersions as stand-ins because they are the most simple kind of
     * Versions. */

  public GremlinVersionSuccessorFactoryTest() throws GroundException {
    super();
  }

  @Test
  public void testVersionSuccessorCreation() throws GroundException {
    GremlinConnection connection = null;
    try {
      connection = super.gremlinClient.getConnection();
      String fromNodeId = super.factories.getNodeFactory().create("testFromId").getId();
      String toNodeId = super.factories.getNodeFactory().create("testToId").getId();

      String fromId = super.createNodeVersion(fromNodeId);
      String toId = super.createNodeVersion(toNodeId);

      String vsId = super.versionSuccessorFactory.create(connection, fromId, toId).getId();

      VersionSuccessor<?> retrieved = super.versionSuccessorFactory.retrieveFromDatabase(
          connection, vsId);

      assertEquals(fromId, retrieved.getFromId());
      assertEquals(toId, retrieved.getToId());
    } finally {
      connection.abort();
    }
  }

  @Test(expected = GroundException.class)
  public void testBadVersionSuccesorCreation() throws GroundException {
    GremlinConnection connection = null;
    try {
      String toId = null;

      try {
        connection = super.gremlinClient.getConnection();
        String toNodeId = super.factories.getNodeFactory().create("testToId").getId();
        toId = super.createNodeVersion(toNodeId);
      } catch (GroundException ge) {
        fail(ge.getMessage());
      }

      // this statement should be fail because the fromId does not exist
      super.versionSuccessorFactory.create(connection, "someBadId", toId).getId();
    } finally {
      connection.abort();
    }
  }
}
