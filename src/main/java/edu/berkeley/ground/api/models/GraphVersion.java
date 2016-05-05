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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class GraphVersion extends RichVersion {
    private static final Logger LOGGER = LoggerFactory.getLogger(GraphVersion.class);

    // the id of the Graph that contains this Version
    private String graphId;

    // the list of ids of EdgeVersions in this GraphVersion
    private List<String> edgeVersionIds;

    @JsonCreator
    protected GraphVersion(@JsonProperty("id") String id,
                           @JsonProperty("tags") Optional<Map<String, Tag>> tags,
                           @JsonProperty("structureVersionId") Optional<String> structureVersionId,
                           @JsonProperty("reference") Optional<String> reference,
                           @JsonProperty("parameters") Optional<Map<String, String>> parameters,
                           @JsonProperty("graphId") String graphId,
                           @JsonProperty("edgeVersionIds") List<String> edgeVersionIds)  {

        super(id, tags, structureVersionId, reference, parameters);

        this.graphId = graphId;
        this.edgeVersionIds = edgeVersionIds;
    }

    @JsonProperty
    public String getGraphId() {
        return this.graphId;
    }

    @JsonProperty
    public List<String> getEdgeVersionIds() {
        return this.edgeVersionIds;
    }

}
