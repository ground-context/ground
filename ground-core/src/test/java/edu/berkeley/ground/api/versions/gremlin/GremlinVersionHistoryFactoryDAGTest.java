package edu.berkeley.ground.api.versions.gremlin;

import org.junit.Test;

import edu.berkeley.ground.api.GremlinTest;
import edu.berkeley.ground.api.versions.VersionHistoryDAG;
import edu.berkeley.ground.api.versions.VersionSuccessor;
import edu.berkeley.ground.db.GremlinClient.GremlinConnection;
import edu.berkeley.ground.exceptions.GroundException;
import static org.junit.Assert.*;

public class GremlinVersionHistoryFactoryDAGTest extends GremlinTest {

    public GremlinVersionHistoryFactoryDAGTest() throws GroundException{
        super();
    }

    @Test
    public void testVersionHistoryDAGCreation() {
        try {
            String testId = "Nodes.test";
            super.versionHistoryDAGFactory.create(testId);
            GremlinConnection connection = super.gremlinClient.getConnection();

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
            GremlinConnection connection = super.gremlinClient.getConnection();
            String testId = "Nodes.test";
            super.versionHistoryDAGFactory.create(testId);

            VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(connection,
                    testId);

            String fromId = super.createNodeVersion();
            String toId = super.createNodeVersion();

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
        String fromId = "someRandomId";
        String toId = null;
        GremlinConnection connection = null;

        // None of these statements should throw an error.
        try {
            connection = super.gremlinClient.getConnection();
            super.versionHistoryDAGFactory.create(testId);

            dag = super.versionHistoryDAGFactory.retrieveFromDatabase(connection, testId);
            toId = super.createNodeVersion();
        } catch (GroundException ge) {
            fail(ge.getMessage());
        }

        // This should fail because there is no version with fromId
        super.versionHistoryDAGFactory.addEdge(connection, dag, fromId, toId, testId);
    }
}
