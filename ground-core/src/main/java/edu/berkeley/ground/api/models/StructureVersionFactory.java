package edu.berkeley.ground.api.models;

import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.Map;
import java.util.Optional;

public abstract class StructureVersionFactory {
    public abstract StructureVersion create(String structureId,
                                            Map<String, GroundType> attributes,
                                            Optional<String> parentId) throws GroundException;

    public abstract StructureVersion retrieveFromDatabase(String id) throws GroundException;

    protected static StructureVersion construct(String id, String structureId, Map<String, GroundType> attributes) {
        return new StructureVersion(id, structureId, attributes);
    }
}
