package edu.berkeley.ground.api.versions;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Item<T extends Version> {

    private String id;

    protected Item(String id) {
        this.id = id;
    }

    @JsonProperty
    public String getId() {
        return this.id;
    }

}
