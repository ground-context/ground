package edu.berkeley.ground.api.versions;

import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.ArrayList;
import java.util.List;

public abstract class VersionHistoryDAGFactory {
    public abstract <T extends Version> VersionHistoryDAG<T> create(String itemId) throws GroundException;

    public abstract <T extends Version> VersionHistoryDAG<T> retrieveFromDatabase(GroundDBConnection connection, String itemId) throws GroundException;

    public abstract void addEdge(GroundDBConnection connection, VersionHistoryDAG dag, String parentId, String childId, String itemId) throws GroundException;

    protected static <T extends Version> VersionHistoryDAG<T> construct(String itemId) {
        return new VersionHistoryDAG<>(itemId, new ArrayList<>());
    }

    protected static <T extends Version> VersionHistoryDAG<T> construct(String itemId, List<VersionSuccessor<T>> edges) {
        return new VersionHistoryDAG<>(itemId, edges);
    }
}
