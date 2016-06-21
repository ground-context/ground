package edu.berkeley.ground.api.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.berkeley.ground.api.versions.Item;

public class Edge extends Item<EdgeVersion> {
    // the name of this Edge
    private String name;

    @JsonCreator
    Edge(@JsonProperty("id") String id,
         @JsonProperty("name") String name) {
        super(id);

        this.name = name;
    }

    @JsonProperty
    public String getName() {
        return this.name;
    }

    public static String idToName(String id) {
        return id.substring(6);
    }
}
