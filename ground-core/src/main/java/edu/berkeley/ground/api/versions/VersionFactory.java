package edu.berkeley.ground.api.versions;

import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.exceptions.GroundException;

public abstract class VersionFactory {
    public abstract void insertIntoDatabase(GroundDBConnection connection, String id) throws GroundException;
}
