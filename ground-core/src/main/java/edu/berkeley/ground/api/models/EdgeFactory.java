package edu.berkeley.ground.api.models;

import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.List;

public abstract class EdgeFactory {
    public abstract Edge create(String name) throws GroundException;

    public abstract Edge retrieveFromDatabase(String name) throws GroundException;

    public abstract void update(GroundDBConnection connection, String itemId, String childId, List<String> parentIds) throws GroundException;

    protected static Edge construct(String id, String name) {
        return new Edge(id, name);
    }
}
