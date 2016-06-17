package edu.berkeley.ground.api.versions;

import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.Optional;

public abstract class ItemFactory {
    public abstract void insertIntoDatabase(GroundDBConnection connection, String id) throws GroundException;

    public abstract void update(GroundDBConnection connection, String itemId, String childId, Optional<String> parent) throws GroundException;
}
