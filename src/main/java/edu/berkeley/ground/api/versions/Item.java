package edu.berkeley.ground.api.versions;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.exceptions.GroundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Item<T extends Version> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Item.class);

    private String id;

    protected Item(String id) {
        this.id = id;
    }

    @JsonProperty
    public String getId() {
        return this.id;
    }

    private List<String> getLeaves(GroundDBConnection connection) throws GroundException {
        try {
            VersionHistoryDAG<T> dag = VersionHistoryDAG.retrieveFromDatabase(connection, this.id);

            return dag.getLeaves();
        } catch (GroundException e) {
            if (!e.getMessage().contains("No results found for query:")) {
                throw e;
            }

            return new ArrayList<>();
        }
    }

    /**
     * Updates this Item to have item as a child of parent.
     *
     * @param itemId the new Version
     * @param parent the parent of item
     */
    public void update(GroundDBConnection connection, String itemId, Optional<String> parent) throws GroundException {
        String parentId;
        // If a parent is specified, great. If it's not specified and there is only one leaf, great.
        // If it's not specified, there are either 0 or > 1 leaves, then make it a child of EMPTY.
        // Eventually, there should be a specification about empty-child?
        if (parent.isPresent()) {
            parentId = parent.get();
        } else {
            List<String> leaves = this.getLeaves(connection);
            if (leaves.size() == 1) {
                parentId = leaves.get(0);
            } else {
                parentId = "EMPTY";
            }
        }

        VersionHistoryDAG<T> dag;
        try {
             dag = VersionHistoryDAG.retrieveFromDatabase(connection, this.id);
        } catch (GroundException e) {
            if (!e.getMessage().contains("No results found for query:")) {
                throw e;
            }

            dag = VersionHistoryDAG.create(connection, this.id);
        }

        if (parent.isPresent() && !dag.checkItemInDag(parentId)) {
            String errorString = "Parent " + parent + " is not in Item " + this.getId() + ".";

            LOGGER.error(errorString);
            throw new GroundException(errorString);
        }

        dag.addEdge(connection, parentId, itemId);
    }

    /* FACTORY METHODS */

    public static void insertIntoDatabase(GroundDBConnection connection, String id) throws GroundException {
        List<DbDataContainer> insertions = new ArrayList<>();
        insertions.add(new DbDataContainer("id", Type.STRING, id));

        connection.insert("Items", insertions);
    }
}
