package edu.berkeley.ground.api.versions;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.exceptions.GroundDBException;
import edu.berkeley.ground.exceptions.GroundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VersionHistoryDAG<T extends Version> {
    private static final Logger LOGGER = LoggerFactory.getLogger(VersionHistoryDAG.class);

    // the id of the Version that's at the rootId of this DAG
    private String itemId;

    // list of VersionSuccessors that make up this DAG
    private List<Long> edgeIds;

    // map of parents to children
    private Map<String, String> parentChildMap;

    protected VersionHistoryDAG(String itemId, List<VersionSuccessor<T>> edges) {
        this.itemId = itemId;
        this.edgeIds = new ArrayList<>();
        this.parentChildMap = new HashMap<>();

        for (VersionSuccessor<T> edge : edges) {
            edgeIds.add(edge.getId());
            parentChildMap.put(edge.getFromId(), edge.getToId());
        }
    }

    @JsonProperty
    public String getItemId() {
        return this.itemId;
    }

    @JsonProperty
    public List<Long> getEdgeIds() {
        return this.edgeIds;
    }

    /**
     * Checks if a given ID is in the DAG.
     *
     * @param id the ID to be checked
     * @return true if id is in the DAG, false otherwise
     */
    public boolean checkItemInDag(String id) {
        return this.parentChildMap.keySet().contains(id) || this.parentChildMap.values().contains(id);
    }

    /**
     * Adds an edge to this DAG.
     *
     * @param connection
     * @param parentId the id of the "from" of the edge
     * @param childId the id of the "to" of the edge
     * @throws GroundException
     */
    public void addEdge(GroundDBConnection connection, String parentId, String childId) throws GroundException {
        VersionSuccessor successor = VersionSuccessor.create(connection, parentId, childId);

        edgeIds.add(successor.getId());
        parentChildMap.put(parentId, childId);

        List<DbDataContainer> insertions = new ArrayList<>();
        insertions.add(new DbDataContainer("item_id", Type.STRING, this.itemId));
        insertions.add(new DbDataContainer("successor_id", Type.INTEGER, (int) successor.getId()));

        connection.insert("VersionHistoryDAGs", insertions);
    }

    /**
     * Returns the leaves of the DAG (i.e., any version id that is not a parent of another version id).
     *
     * @return the list of the IDs of the leaves of this DAG
     */
    protected List<String> getLeaves() {
        List<String> leaves = new ArrayList<>(this.parentChildMap.values());
        leaves.removeAll(this.parentChildMap.keySet());

        return leaves;
    }

    /* FACTORY METHODS */
    public static <T extends Version>  VersionHistoryDAG<T> create(GroundDBConnection connection, String itemId) throws GroundException {
        return new VersionHistoryDAG<>(itemId, new ArrayList<>());
    }

    public static <T extends Version> VersionHistoryDAG<T> retrieveFromDatabase(GroundDBConnection connection, String itemId) throws GroundException {
        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("item_id", Type.STRING, itemId));

        ResultSet resultSet = connection.equalitySelect("VersionHistoryDAGs", DBClient.SELECT_STAR, predicates);

        List<VersionSuccessor<T>> edges = new ArrayList<>();
        try {
            do {
                edges.add(VersionSuccessor.retrieveFromDatabase(connection, resultSet.getInt(2)));
            } while (resultSet.next());
        } catch (SQLException e) {
            LOGGER.error("Unexpected error: " + e.getMessage());

            throw new GroundDBException(e.getMessage());
        }

        return new VersionHistoryDAG<>(itemId, edges);
    }
}
