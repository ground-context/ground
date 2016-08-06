package edu.berkeley.ground.api.models;

import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.List;

public abstract class StructureFactory {
    public abstract Structure create(String name) throws GroundException;

    public abstract Structure retrieveFromDatabase(String name)  throws GroundException;

    public abstract void update(GroundDBConnection connection, String itemId, String childId, List<String> parentIds) throws GroundException;

    protected static Structure construct(String id, String name) {
        return new Structure(id, name);
    }
}
