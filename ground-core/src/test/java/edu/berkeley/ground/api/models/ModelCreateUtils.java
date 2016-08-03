package edu.berkeley.ground.api.models;

import edu.berkeley.ground.api.versions.GroundType;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ModelCreateUtils {
    public static NodeVersion getNodeVersion(String id, Optional<Map<String, Tag>> tags, Optional<String> structureVersionId, Optional<String> reference, Optional<Map<String, String>> parameters, String nodeId) {
        return new NodeVersion(id, tags, structureVersionId, reference, parameters, nodeId);
    }

    public static EdgeVersion getEdgeVersion(String id, String edgeId, String fromId, String toId) {
        return new EdgeVersion(id, Optional.<Map<String,Tag>>empty(), Optional.<String>empty(), Optional.<String>empty(), Optional.<Map<String,String>>empty(), edgeId, fromId, toId);
    }

    public static GraphVersion getGraphVersion(String id, String graphId, List<String> edgeVersionIds) {
        return new GraphVersion(id, Optional.<Map<String,Tag>>empty(), Optional.<String>empty(), Optional.<String>empty(), Optional.<Map<String,String>>empty(), graphId, edgeVersionIds);
    }

    public static StructureVersion getStructureVersion(String id, String structureId, Map<String, GroundType> attributes) {
        return new StructureVersion(id,structureId, attributes);
    }
}
