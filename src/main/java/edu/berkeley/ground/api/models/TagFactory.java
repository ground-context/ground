package edu.berkeley.ground.api.models;

import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.Map;

public abstract class TagFactory {
    public abstract Map<String, Tag> retrieveFromDatabaseById(GroundDBConnection connection, String id) throws GroundException;
}
