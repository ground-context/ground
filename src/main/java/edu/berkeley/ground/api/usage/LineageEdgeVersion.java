package edu.berkeley.ground.api.usage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.Tag;
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

public class LineageEdgeVersion extends RichVersion {
    private static final Logger LOGGER = LoggerFactory.getLogger(LineageEdgeVersion.class);

    // the id of the LineageEdge containing this Version
    private String lineageEdgeId;

    // the id of the RichVersion that this LineageEdgeVersion originates from
    private String fromId;

    // the id of the RichVersion that this LineageEdgeVersion points to
    private String toId;

    @JsonCreator
    protected LineageEdgeVersion(@JsonProperty("id") String id,
                                 @JsonProperty("tags") Optional<Map<String, Tag>> tags,
                                 @JsonProperty("structureVersionId") Optional<String> structureVersionId,
                                 @JsonProperty("reference") Optional<String> reference,
                                 @JsonProperty("parameters") Optional<Map<String, String>> parameters,
                                 @JsonProperty("fromId") String fromId,
                                 @JsonProperty("toId") String toId,
                                 @JsonProperty("lineageEdgeId") String lineageEdgeId) {
        super(id, tags, structureVersionId, reference, parameters);

        this.lineageEdgeId = lineageEdgeId;
        this.fromId = fromId;
        this.toId = toId;
    }

    @JsonProperty
    public String getLineageEdgeId() {
        return this.lineageEdgeId;
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
    public static LineageEdgeVersion create(GroundDBConnection connection,
                                            Optional<Map<String, Tag>> tags,
                                            Optional<String> structureVersionId,
                                            Optional<String> reference,
                                            Optional<Map<String, String>> parameters,
                                            String fromId,
                                            String toId,
                                            String lineageEdgeId,
                                            Optional<String> parentId) throws GroundException {

        LineageEdge lineageEdge = LineageEdge.retrieveFromDatabase(connection, LineageEdge.idToName(lineageEdgeId));

        String id = IdGenerator.generateId(lineageEdgeId);

        tags = tags.map ( tagsMap ->
                                  tagsMap.values().stream().collect(Collectors.toMap(Tag::getKey, tag -> new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType())))
        );

        RichVersion.insertIntoDatabase(connection, id, tags, structureVersionId, reference, parameters);

        List<DbDataContainer> insertions = new ArrayList<>();
        insertions.add(new DbDataContainer("id", Type.STRING, id));
        insertions.add(new DbDataContainer("lineageedge_id", Type.STRING, lineageEdgeId));
        insertions.add(new DbDataContainer("endpoint_one", Type.STRING, fromId));
        insertions.add(new DbDataContainer("endpoint_two", Type.STRING, toId));

        connection.insert("LineageEdgeVersions", insertions);

        lineageEdge.update(connection, id, parentId);

        LOGGER.info("Created lineage edge version " + id + " in lineage edge " + lineageEdgeId + ".");

        return new LineageEdgeVersion(id, tags, structureVersionId, reference, parameters, fromId, toId, lineageEdgeId);
    }

    public static LineageEdgeVersion retrieveFromDatabase(GroundDBConnection connection, String id) throws GroundException {
        RichVersion version = RichVersion.retrieveFromDatabase(connection, id);

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("id", Type.STRING, id));

        ResultSet resultSet = connection.equalitySelect("LineageEdgeVersions", DBClient.SELECT_STAR, predicates);

        String lineageEdgeId = DbUtils.getString(resultSet, 2);
        String fromId = DbUtils.getString(resultSet, 3);
        String toId = DbUtils.getString(resultSet, 4);

        LOGGER.info("Retrieved lineage edge version " + id + " in lineage edge " + lineageEdgeId + ".");

        return new LineageEdgeVersion(id, version.getTags(), version.getStructureVersionId(), version.getReference(), version.getParameters(), fromId, toId, lineageEdgeId);
    }
}
