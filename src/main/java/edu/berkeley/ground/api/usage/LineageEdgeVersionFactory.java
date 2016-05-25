package edu.berkeley.ground.api.usage;

import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.Map;
import java.util.Optional;

public abstract class LineageEdgeVersionFactory {
    public abstract LineageEdgeVersion create(Optional<Map<String, Tag>> tags,
                                              Optional<String> structureVersionId,
                                              Optional<String> reference,
                                              Optional<Map<String, String>> parameters,
                                              String fromId,
                                              String toId,
                                              String lineageEdgeId,
                                              Optional<String> parentId) throws GroundException;

    public abstract LineageEdgeVersion retrieveFromDatabase(String id) throws GroundException;

    protected static LineageEdgeVersion construct(String id,
                                                  Optional<Map<String, Tag>> tags,
                                                  Optional<String> structureVersionId,
                                                  Optional<String> reference,
                                                  Optional<Map<String, String>> parameters,
                                                  String fromId,
                                                  String toId,
                                                  String lineageEdgeId) {
        return new LineageEdgeVersion(id, tags, structureVersionId, reference, parameters, fromId, toId, lineageEdgeId);
    }
}
