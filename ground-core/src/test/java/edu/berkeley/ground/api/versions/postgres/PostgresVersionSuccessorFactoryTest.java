package edu.berkeley.ground.api.versions.postgres;

import org.junit.Test;

import edu.berkeley.ground.api.PostgresTest;
import edu.berkeley.ground.api.versions.VersionSuccessor;
import edu.berkeley.ground.db.PostgresClient.PostgresConnection;
import edu.berkeley.ground.exceptions.GroundException;
import static org.junit.Assert.*;

public class PostgresVersionSuccessorFactoryTest extends PostgresTest {

    public PostgresVersionSuccessorFactoryTest() throws GroundException {
        super();
    }

    @Test
    public void testVersionSuccessorCreation() {
        try {
            String fromId = "testFromId";
            String toId = "testToId";

            PostgresConnection connection = super.cassandraClient.getConnection();
            super.versionFactory.insertIntoDatabase(connection, fromId);
            super.versionFactory.insertIntoDatabase(connection, toId);

            VersionSuccessor<?> successor = super.versionSuccessorFactory.create(connection, fromId, toId);

            VersionSuccessor<?> retrieved = super.versionSuccessorFactory.retrieveFromDatabase(connection,
                    successor.getId());

            assertEquals(fromId, retrieved.getFromId());
            assertEquals(toId, retrieved.getToId());
        } catch (GroundException ge) {
            fail(ge.getMessage());
        }
    }

    @Test(expected = GroundException.class)
    public void testBadVersionSuccessorCreation() throws GroundException {
        String fromId = "testFromId";
        String toId = "testToId";
        PostgresConnection connection = null;

        // Catch exceptions for these two lines because they should not fal
        try {
            // the main difference is that we're not creating a Version for the toId
            connection = super.cassandraClient.getConnection();
            super.versionFactory.insertIntoDatabase(connection, fromId);
        } catch (GroundException ge) {
            fail(ge.getMessage());
        }

        // This statement should fail because toId is not in the database
        super.versionSuccessorFactory.create(connection, fromId, toId);
    }
}
