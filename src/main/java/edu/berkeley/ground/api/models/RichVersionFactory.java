package edu.berkeley.ground.api.models;

import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.Map;
import java.util.Optional;

public abstract class RichVersionFactory {
    public abstract void insertIntoDatabase(GroundDBConnection connection, String id, Optional<Map<String, Tag>>tags, Optional<String> structureVersionId, Optional<String> reference, Optional<Map<String, String>> parameters) throws GroundException;

    public abstract RichVersion retrieveFromDatabase(GroundDBConnection connection, String id) throws GroundException;

    protected static RichVersion construct(String id, Optional<Map<String, Tag>>tags, Optional<String> structureVersionId, Optional<String> reference, Optional<Map<String, String>> parameters) {
        return new RichVersion(id, tags, structureVersionId, reference, parameters);
    }

    protected static boolean checkStructureTags(GroundDBConnection connection, StructureVersion structureVersion, Optional<Map<String, Tag>> tags) throws GroundException {
        Map<String, Type> structureVersionAttributes = structureVersion.getAttributes();

        if(!tags.isPresent()) {
            return false;
        }

        Map<String, Tag> tagsMap = tags.get();

        for (String key : structureVersionAttributes.keySet()) {

            // check if such a tag exists
            if(!tagsMap.keySet().contains(key)) {
                return false;
            } else if (!tagsMap.get(key).getValueType().isPresent()) { // check that value type is specified
                return false;
            } else if (!tagsMap.get(key).getValueType().get().equals(structureVersionAttributes.get(key))) { // check that the value type is the same
                return false;
            }
        }

        return true;
    }
}
