package edu.berkeley.ground.api.models;

import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.Map;
import java.util.Optional;

public abstract class StructureVersionFactory {
    public abstract StructureVersion create(GroundDBConnection connection,
                                            String structureId,
                                            Map<String, Type> attributes,
                                            Optional<String> parentId) throws GroundException;

    public abstract StructureVersion retrieveFromDatabase(GroundDBConnection connection, String id) throws GroundException;

    protected static StructureVersion construct(String id, String structureId, Map<String, Type> attributes) {
        return new StructureVersion(id, structureId, attributes);
    }
}
