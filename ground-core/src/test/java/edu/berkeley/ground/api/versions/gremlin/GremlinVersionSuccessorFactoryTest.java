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
    public void testVersionSuccessorCreation() {
        try {
            GremlinConnection connection = super.gremlinClient.getConnection();
            String fromId = super.createNodeVersion();
            String toId = super.createNodeVersion();

            String vsId = super.versionSuccessorFactory.create(connection, fromId, toId).getId();

            VersionSuccessor<?> retrieved = super.versionSuccessorFactory.retrieveFromDatabase(
                    connection, vsId);

            assertEquals(fromId, retrieved.getFromId());
            assertEquals(toId, retrieved.getToId());
        } catch (GroundException ge) {
            fail(ge.getMessage());
        }
    }

    @Test(expected = GroundException.class)
    public void testBadVersionSuccesorCreation() throws GroundException {
        GremlinConnection connection = null;
        String toId = null;

        try {
            connection = super.gremlinClient.getConnection();
            toId = super.createNodeVersion();
        } catch (GroundException ge) {
            fail(ge.getMessage());
        }

        // this statement should be fail because the fromId does not exist
        super.versionSuccessorFactory.create(connection, "someBadId", toId).getId();
    }
}
