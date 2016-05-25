package edu.berkeley.ground.api.usage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.berkeley.ground.api.versions.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LineageEdge extends Item<LineageEdgeVersion> {
    private static final Logger LOGGER = LoggerFactory.getLogger(LineageEdge.class);

    // the name of this LineageEdge
    private String name;

    @JsonCreator
    protected LineageEdge(@JsonProperty("id") String id,
                          @JsonProperty("name") String name) {
        super(id);

        this.name = name;
    }

    @JsonProperty
    public String getName() {
        return this.name;
    }

    public static String idToName(String id) {
        return id.substring(13);
    }
}
