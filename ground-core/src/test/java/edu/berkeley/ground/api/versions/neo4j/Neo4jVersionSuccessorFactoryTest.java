package edu.berkeley.ground.api.versions.neo4j;

import org.junit.Test;

import edu.berkeley.ground.api.Neo4jTest;
import edu.berkeley.ground.api.versions.VersionSuccessor;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.exceptions.GroundException;
import static org.junit.Assert.*;

public class Neo4jVersionSuccessorFactoryTest extends Neo4jTest {

    public Neo4jVersionSuccessorFactoryTest() throws GroundException {
        super();
    }

    @Test
    public void testVersionSuccessorCreation() {
        try {
            Neo4jClient.Neo4jConnection connection = super.neo4jClient.getConnection();
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
        Neo4jClient.Neo4jConnection connection = null;
        String toId = null;

        try {
            connection = super.neo4jClient.getConnection();
            toId = super.createNodeVersion();
        } catch (GroundException ge) {
            fail(ge.getMessage());
        }

        // this statement should be fail because the fromId does not exist
        super.versionSuccessorFactory.create(connection, "someBadId", toId).getId();
    }
}
