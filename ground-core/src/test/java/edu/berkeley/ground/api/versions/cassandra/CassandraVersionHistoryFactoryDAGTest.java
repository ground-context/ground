package edu.berkeley.ground.api.versions.cassandra;

import org.junit.Test;

import edu.berkeley.ground.api.CassandraTest;
import edu.berkeley.ground.api.versions.VersionHistoryDAG;
import edu.berkeley.ground.api.versions.VersionSuccessor;
import edu.berkeley.ground.db.CassandraClient.CassandraConnection;
import edu.berkeley.ground.exceptions.GroundDBException;
import edu.berkeley.ground.exceptions.GroundException;
import static org.junit.Assert.*;

// TODO: Close connections to avoid littering.
public class CassandraVersionHistoryFactoryDAGTest extends CassandraTest {

    public CassandraVersionHistoryFactoryDAGTest() throws GroundDBException {
        super();
    }

    @Test
    public void testVersionHistoryDAGCreation() {
        try {
            String testId = "Nodes.test";
            super.versionHistoryDAGFactory.create(testId);
            CassandraConnection connection = super.cassandraClient.getConnection();

            VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(connection,
                    testId);

            assertEquals(0, dag.getEdgeIds().size());

            connection.abort();
        } catch (GroundException ge) {
            fail(ge.getMessage());
        }
    }

    @Test
    public void testAddEdge() {
        try {
            String testId = "Nodes.test";
            CassandraConnection connection = super.cassandraClient.getConnection();
            super.versionHistoryDAGFactory.create(testId);

            VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(connection,
                    testId);

            String fromId = "testFromId";
            String toId = "testToId";

            super.versionFactory.insertIntoDatabase(connection, fromId);
            super.versionFactory.insertIntoDatabase(connection, toId);

            super.versionHistoryDAGFactory.addEdge(connection, dag, fromId, toId, testId);

            VersionHistoryDAG<?> retrieved = super.versionHistoryDAGFactory.retrieveFromDatabase(connection,
                    testId);

            assertEquals(1, retrieved.getEdgeIds().size());
            assertEquals(toId, retrieved.getLeaves().get(0));

            VersionSuccessor<?> successor = super.versionSuccessorFactory.retrieveFromDatabase(
                    connection, retrieved.getEdgeIds().get(0));

            assertEquals(fromId , successor.getFromId());
            assertEquals(toId, successor.getToId());
        } catch (GroundException ge) {
            fail(ge.getMessage());
        }
    }

    @Test(expected = GroundException.class)
    public void testAddEdgeWithBadParent() throws GroundException {
        String testId = "Nodes.test";
        VersionHistoryDAG<?> dag = null;
        String fromId = "testFromId";
        String toId = "testToId";
        CassandraConnection connection = null;

        // None of these statements should throw an error.
        try {
            connection = super.cassandraClient.getConnection();
            super.versionHistoryDAGFactory.create(testId);

            dag = super.versionHistoryDAGFactory.retrieveFromDatabase(connection, testId);

            super.versionFactory.insertIntoDatabase(connection, toId);
        } catch (GroundException ge) {
            fail(ge.getMessage());
        }

        // This should fail because there is no version with fromId
        super.versionHistoryDAGFactory.addEdge(connection, dag, fromId, toId, testId);
    }
}
