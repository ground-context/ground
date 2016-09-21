package edu.berkeley.ground.api.versions.neo4j;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import edu.berkeley.ground.api.Neo4jTest;
import edu.berkeley.ground.api.versions.VersionHistoryDAG;
import edu.berkeley.ground.api.versions.VersionSuccessor;
import edu.berkeley.ground.db.Neo4jClient.Neo4jConnection;
import edu.berkeley.ground.exceptions.GroundException;
import static org.junit.Assert.*;

public class Neo4jItemFactoryTest extends Neo4jTest {
    public Neo4jItemFactoryTest() throws GroundException {
        super();
    }

    @Test
    public void testCorrectUpdateWithParent() throws GroundException {
        Neo4jConnection connection = null;
        try {
            String testId = super.factories.getNodeFactory().create("test").getId();
            connection = super.neo4jClient.getConnection();

            super.itemFactory.insertIntoDatabase(connection, testId);

            String fromNodeId = super.factories.getNodeFactory().create("testFromNode").getId();
            String toNodeId = super.factories.getNodeFactory().create("testToNode").getId();
            String fromId = super.createNodeVersion(fromNodeId);
            String toId = super.createNodeVersion(toNodeId);

            List<String> parentIds = new ArrayList<>();

            super.itemFactory.update(connection, testId, fromId, parentIds);

            parentIds.clear();
            parentIds.add(fromId);
            super.itemFactory.update(connection, testId, toId, parentIds);

            VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(connection,
                    testId);

            assertEquals(2, dag.getEdgeIds().size());
            assertEquals(toId, dag.getLeaves().get(0));

            VersionSuccessor<?> successor = null;
            for (String id : dag.getEdgeIds()) {
                successor = super.versionSuccessorFactory.retrieveFromDatabase(connection, id);

                if (!successor.getFromId().equals(testId)) {
                    break;
                }
            }

            assertEquals(fromId, successor.getFromId());
            assertEquals(toId, successor.getToId());
        } finally {
            connection.abort();
        }
    }

    @Test
    public void testCorrectUpdateWithoutParent() throws GroundException {
        Neo4jConnection connection = null;
        try {
            String testId = super.factories.getNodeFactory().create("test").getId();
            connection = super.neo4jClient.getConnection();

            String toNodeId = super.factories.getNodeFactory().create("testToNode").getId();
            String toId = super.createNodeVersion(toNodeId);

            List<String> parentIds = new ArrayList<>();

            // No parent is specified, and there is no other version in this Item, we should
            // automatically make this a child of EMPTY
            super.itemFactory.update(connection, testId, toId, parentIds);

            VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(connection,
                    testId);

            assertEquals(1, dag.getEdgeIds().size());
            assertEquals(toId, dag.getLeaves().get(0));

            VersionSuccessor<?> successor = super.versionSuccessorFactory.retrieveFromDatabase(
                    connection, dag.getEdgeIds().get(0));

            assertEquals(testId , successor.getFromId());
            assertEquals(toId, successor.getToId());
        } finally {
            connection.abort();
        }
    }

    @Test
    public void testCorrectUpdateWithLinearHistory() throws GroundException {
        Neo4jConnection connection = null;
        try {
            String testId = super.factories.getNodeFactory().create("test").getId();
            connection = super.neo4jClient.getConnection();

            super.itemFactory.insertIntoDatabase(connection, testId);

            String fromNodeId = super.factories.getNodeFactory().create("testFromNode").getId();
            String toNodeId = super.factories.getNodeFactory().create("testToNode").getId();


            String fromId = super.createNodeVersion(fromNodeId);
            String toId = super.createNodeVersion(toNodeId);

            List<String> parentIds = new ArrayList<>();

            // first, make from a child of EMPTY
            super.itemFactory.update(connection, testId, fromId, parentIds);

            // then, add to as a child and make sure that it becomes a child of from
            parentIds.clear();
            parentIds.add(fromId);
            super.itemFactory.update(connection, testId, toId, parentIds);

            VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(connection,
                    testId);

            assertEquals(toId, dag.getLeaves().get(0));

            VersionSuccessor<?> fromSuccessor = super.versionSuccessorFactory.retrieveFromDatabase(
                    connection, dag.getEdgeIds().get(0));

            VersionSuccessor<?> toSuccessor = super.versionSuccessorFactory.retrieveFromDatabase(
                    connection, dag.getEdgeIds().get(1));

            if (!fromSuccessor.getFromId().equals(testId)) {
                VersionSuccessor<?> tmp = fromSuccessor;
                fromSuccessor = toSuccessor;
                toSuccessor = tmp;
            }

            assertEquals(testId , fromSuccessor.getFromId());
            assertEquals(fromId, fromSuccessor.getToId());

            assertEquals(fromId, toSuccessor.getFromId());
            assertEquals(toId, toSuccessor.getToId());
        } finally {
            connection.abort();
        }
    }

    @Test(expected = GroundException.class)
    public void testIncorrectUpdate() throws GroundException {
        Neo4jConnection connection = null;

        try {
            String testId = super.factories.getNodeFactory().create("test").getId();
            String fromId = "someRandomId";
            String toId = null;

            try {
                connection = super.neo4jClient.getConnection();
                super.itemFactory.insertIntoDatabase(connection, testId);

                String nodeToId = super.factories.getNodeFactory().create("testToNode").getId();
                toId = super.createNodeVersion(nodeToId);
            } catch (GroundException ge) {
                fail(ge.getMessage());
            }

            List<String> parentIds = new ArrayList<>();
            parentIds.add(fromId);

            // this should fail because fromId is not a valid version
            super.itemFactory.update(connection, testId, toId, parentIds);
        } finally {
            connection.abort();
        }
    }

    @Test
    public void testMultipleParents() throws GroundException {
        Neo4jConnection connection = null;
        try {
            String testId = super.factories.getNodeFactory().create("test").getId();
            connection = super.neo4jClient.getConnection();

            super.itemFactory.insertIntoDatabase(connection, testId);

            String parentOneNodeId = super.factories.getNodeFactory().create("testParentOneNode").getId();
            String parentTwoNodeId = super.factories.getNodeFactory().create("testParentTwoNode").getId();

            String parentOne = super.createNodeVersion(parentOneNodeId);
            String parentTwo = super.createNodeVersion(parentTwoNodeId);

            String childNodeId = super.factories.getNodeFactory().create("testChildNode").getId();
            String child = super.createNodeVersion(childNodeId);

            List<String> parentIds = new ArrayList<>();

            // first, make the parents children of EMPTY
            super.itemFactory.update(connection, testId, parentOne, parentIds);
            super.itemFactory.update(connection, testId, parentTwo, parentIds);

            // then, add to as a child and make sure that it becomes a child of from
            parentIds.clear();
            parentIds.add(parentOne);
            parentIds.add(parentTwo);

            super.itemFactory.update(connection, testId, child, parentIds);

            VersionHistoryDAG<?> dag = super.versionHistoryDAGFactory.retrieveFromDatabase(connection,
                    testId);

            assertEquals(4, dag.getEdgeIds().size());
            assertEquals(child, dag.getLeaves().get(0));
        } finally {
            connection.abort();
        }
    }
}
