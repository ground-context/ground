package edu.berkeley.ground.api.versions;

import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.exceptions.GroundException;

public abstract class VersionSuccessorFactory {
    public abstract <T extends Version> VersionSuccessor<T> create(GroundDBConnection connection, String fromId, String toId) throws GroundException;

    public abstract <T extends Version> VersionSuccessor<T> retrieveFromDatabase(GroundDBConnection connection, int dbId) throws GroundException;


    protected static <T extends Version> VersionSuccessor<T> construct(long id, String fromId, String toId) {
        return new VersionSuccessor<>(id, fromId, toId);
    }
}
