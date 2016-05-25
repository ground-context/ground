package edu.berkeley.ground.api.versions;

import com.fasterxml.jackson.annotation.JsonProperty;


public class VersionSuccessor<T extends Version> {

    // the unique id of this VersionSuccessor
    private String id;

    // the id of the Version that originates this successor
    private String fromId;

    // the id of the Version that this success points to
    private String toId;

    protected VersionSuccessor(String id, String fromId, String toId) {
        this.id = id;
        this.fromId = fromId;
        this.toId = toId;
    }

    @JsonProperty
    public String getId() {
        return this.id;
    }

    @JsonProperty
    public String getFromId() {
        return this.fromId;
    }

    public String getToId() {
        return this.toId;
    }


}
