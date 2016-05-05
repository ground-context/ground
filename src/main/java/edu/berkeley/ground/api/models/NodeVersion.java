package edu.berkeley.ground.api.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.DbUtils;
import edu.berkeley.ground.util.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.util.*;
import java.util.stream.Collectors;

public class NodeVersion extends RichVersion {
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeVersion.class);

    // the id of the Node containing this Version
    private String nodeId;

    @JsonCreator
    protected NodeVersion(
            @JsonProperty("id") String id,
            @JsonProperty("tags") Optional<Map<String, Tag>> tags,
            @JsonProperty("structureVersionId") Optional<String> structureVersionId,
            @JsonProperty("reference") Optional<String> reference,
            @JsonProperty("parameters") Optional<Map<String, String>> parameters,
            @JsonProperty("nodeId") String nodeId) {

        super(id, tags, structureVersionId, reference, parameters);

        this.nodeId = nodeId;
    }

    @JsonProperty
    public String getNodeId() {
        return this.nodeId;
    }

}
