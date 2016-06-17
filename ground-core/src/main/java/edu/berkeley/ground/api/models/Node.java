package edu.berkeley.ground.api.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.berkeley.ground.api.versions.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Node extends Item<NodeVersion> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Node.class);

    // the name of this Node
    private String name;

    @JsonCreator
    protected Node(
            @JsonProperty("id") String id,
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
