package edu.berkeley.ground.api.versions;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VersionHistoryDAG<T extends Version> {
    // the id of the Version that's at the rootId of this DAG
    private String itemId;

    // list of VersionSuccessors that make up this DAG
    private List<String> edgeIds;

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
    public List<String> getEdgeIds() {
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
     * @param parentId the id of the "from" of the edge
     * @param childId the id of the "to" of the edge
     * @throws GroundException
     */
    public void addEdge(String parentId, String childId, String successorId) throws GroundException {
        edgeIds.add(successorId);
        parentChildMap.put(parentId, childId);
    }

    /**
     * Returns the leaves of the DAG (i.e., any version id that is not a parent of another version id).
     *
     * @return the list of the IDs of the leaves of this DAG
     */
    public List<String> getLeaves() {
        List<String> leaves = new ArrayList<>(this.parentChildMap.values());
        leaves.removeAll(this.parentChildMap.keySet());

        return leaves;
    }
}
