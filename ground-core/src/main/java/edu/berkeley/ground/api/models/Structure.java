package edu.berkeley.ground.api.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.berkeley.ground.api.versions.Item;

public class Structure extends Item<StructureVersion> {
    // the name of this Structure
    private String name;

    @JsonCreator
    protected Structure(@JsonProperty("id") String id,
                        @JsonProperty("name") String name) {
        super(id);

        this.name = name;
    }

    @JsonProperty
    public String getName() {
        return this.name;
    }

    public static String idToName(String id) {
        return id.substring(11);
    }
}
