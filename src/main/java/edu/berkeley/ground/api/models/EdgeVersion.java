package edu.berkeley.ground.api.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Optional;

public class EdgeVersion extends RichVersion {
    // the id of the Edge containing this Version
    private String edgeId;

    // the id of the NodeVersion that this EdgeVersion originates from
    private String fromId;

    // the id of the NodeVersion that this EdgeVersion points to
    private String toId;

    @JsonCreator
    protected EdgeVersion(
            @JsonProperty("id") String id,
            @JsonProperty("tags") Optional<Map<String, Tag>> tags,
            @JsonProperty("structureVersionId") Optional<String> structureVersionId,
            @JsonProperty("reference") Optional<String> reference,
            @JsonProperty("parameters") Optional<Map<String, String>> parameters,
            @JsonProperty("edgeId") String edgeId,
            @JsonProperty("fromId") String fromId,
            @JsonProperty("toId") String toId) {

        super(id, tags, structureVersionId, reference, parameters);

        this.edgeId = edgeId;
        this.fromId = fromId;
        this.toId = toId;
    }

    @JsonProperty
    public String getEdgeId() {
        return this.edgeId;
    }

    @JsonProperty
    public String getFromId() {
        return this.fromId;
    }

    @JsonProperty
    public String getToId() {
        return this.toId;
    }

}
