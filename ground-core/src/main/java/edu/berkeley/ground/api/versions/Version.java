package edu.berkeley.ground.api.versions;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Version {
    private String id;

    protected Version(@JsonProperty String id) {
        this.id = id;
    }

    @JsonProperty
    public String getId() {
        return this.id;
    }
}
