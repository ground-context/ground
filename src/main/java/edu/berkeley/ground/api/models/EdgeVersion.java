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

public class EdgeVersion extends RichVersion {
    private static final Logger LOGGER = LoggerFactory.getLogger(EdgeVersion.class);

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

    /* FACTORY METHODS */
    public static EdgeVersion create(GroundDBConnection connection,
                                     Optional<Map<String, Tag>> tags,
                                     Optional<String> structureVersionId,
                                     Optional<String> reference,
                                     Optional<Map<String, String>> parameters,
                                     String edgeId,
                                     String fromId,
                                     String toId,
                                     Optional<String> parentId) throws GroundException {
        Edge edge = Edge.retrieveFromDatabase(connection, Edge.idToName(edgeId));

        String id = IdGenerator.generateId(edgeId);

        tags = tags.map ( tagsMap ->
                                  tagsMap.values().stream().collect(Collectors.toMap(Tag::getKey, tag -> new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType())))
        );

        RichVersion.insertIntoDatabase(connection, id, tags, structureVersionId, reference, parameters);

        List<DbDataContainer> insertions = new ArrayList<>();
        insertions.add(new DbDataContainer("id", Type.STRING, id));
        insertions.add(new DbDataContainer("edge_id", Type.STRING, edgeId));
        insertions.add(new DbDataContainer("endpoint_one", Type.STRING, fromId));
        insertions.add(new DbDataContainer("endpoint_two", Type.STRING, toId));

        connection.insert("EdgeVersions", insertions);

        edge.update(connection, id, parentId);

        LOGGER.info("Created edge version " + id + " in edge " + edgeId + ".");

        return new EdgeVersion(id, tags, structureVersionId, reference, parameters, edgeId, fromId, toId);
    }

    public static EdgeVersion retrieveFromDatabase(GroundDBConnection connection, String id) throws GroundException {
        RichVersion version = RichVersion.retrieveFromDatabase(connection, id);

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("id", Type.STRING, id));

        ResultSet resultSet = connection.equalitySelect("EdgeVersions", DBClient.SELECT_STAR, predicates);
        String edgeId = DbUtils.getString(resultSet, 2);
        String fromId =  DbUtils.getString(resultSet, 3);
        String toId = DbUtils.getString(resultSet, 4);

        LOGGER.info("Retrieved edge version " + id + " in edge " + edgeId + ".");

        return new EdgeVersion(id, version.getTags(), version.getStructureVersionId(), version.getReference(), version.getParameters(), edgeId, fromId, toId);
    }
}
