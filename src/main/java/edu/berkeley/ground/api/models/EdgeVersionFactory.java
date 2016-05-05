package edu.berkeley.ground.api.models;

import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.Map;
import java.util.Optional;

public abstract class EdgeVersionFactory {
    public abstract EdgeVersion create(GroundDBConnection connection,
                                       Optional<Map<String, Tag>> tags,
                                       Optional<String> structureVersionId,
                                       Optional<String> reference,
                                       Optional<Map<String, String>> parameters,
                                       String edgeId,
                                       String fromId,
                                       String toId,
                                       Optional<String> parentId) throws GroundException;


    public abstract EdgeVersion retrieveFromDatabase(GroundDBConnection connection, String id) throws GroundException;

    protected static EdgeVersion construct(String id,
                                           Optional<Map<String, Tag>> tags,
                                           Optional<String> structureVersionId,
                                           Optional<String> reference,
                                           Optional<Map<String, String>> parameters,
                                           String edgeId,
                                           String fromId,
                                           String toId) throws GroundException {

        return new EdgeVersion(id, tags, structureVersionId, reference, parameters, edgeId, fromId, toId);
    }
}
