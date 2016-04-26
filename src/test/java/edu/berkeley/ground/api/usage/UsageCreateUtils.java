package edu.berkeley.ground.api.usage;

import edu.berkeley.ground.api.models.Tag;

import java.util.Map;
import java.util.Optional;

public class UsageCreateUtils {
    public static LineageEdgeVersion getLineageEdgeVersion(String id, String lineageEdgeId, String fromId, String toId) {
        return new LineageEdgeVersion(id, Optional.<Map<String, Tag>>empty(), Optional.<String>empty(), Optional.<String>empty(), Optional.<Map<String, String>>empty(), fromId, toId, lineageEdgeId);
    }
}
