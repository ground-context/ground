package edu.berkeley.ground.api.versions.neo4j;

import edu.berkeley.ground.api.versions.ItemFactory;
import edu.berkeley.ground.api.versions.VersionHistoryDAG;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Neo4jItemFactory extends ItemFactory {
    private Neo4jVersionHistoryDAGFactory versionHistoryDAGFactory;

    public Neo4jItemFactory(Neo4jVersionHistoryDAGFactory versionHistoryDAGFactory) {
        this.versionHistoryDAGFactory = versionHistoryDAGFactory;
    }

    public void insertIntoDatabase(GroundDBConnection connection, String id) throws GroundException {
        // DO NOTHING
    }

    public void update(GroundDBConnection connectionPointer, String itemId, String childId, Optional<String> parent) throws GroundException {
        String parentId;

        // If a parent is specified, great. If it's not specified and there is only one leaf, great.
        // If it's not specified, there are either 0 or > 1 leaves, then make it a child of EMPTY.
        // Eventually, there should be a specification about empty-child?
        if (parent.isPresent()) {
            parentId = parent.get();
        } else {
            List<String> leaves = this.getLeaves(connectionPointer, itemId);
            if (leaves.size() == 1) {
                parentId = leaves.get(0);
            } else {
                parentId = itemId;
            }
        }

        VersionHistoryDAG dag;
        try {
            dag = this.versionHistoryDAGFactory.retrieveFromDatabase(connectionPointer, itemId);
        } catch (GroundException e) {
            if (!e.getMessage().contains("No results found for query")) {
                throw e;
            }

            dag = this.versionHistoryDAGFactory.create(itemId);
        }

        if (parent.isPresent() && !dag.checkItemInDag(parentId)) {
            String errorString = "Parent " + parent + " is not in Item " + itemId + ".";

            throw new GroundException(errorString);
        }

        this.versionHistoryDAGFactory.addEdge(connectionPointer, dag, parentId, childId, itemId);
    }

    private List<String> getLeaves(GroundDBConnection connection, String itemId) throws GroundException {
        try {
            VersionHistoryDAG<?> dag = this.versionHistoryDAGFactory.retrieveFromDatabase(connection, itemId);

            return dag.getLeaves();
        } catch (GroundException e) {
            if (!e.getMessage().contains("No results found for query")) {
                throw e;
            }

            return new ArrayList<>();
        }
    }

}
