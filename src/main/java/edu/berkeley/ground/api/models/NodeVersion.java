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

    /* FACTORY METHODS */
    public static NodeVersion create(GroundDBConnection connection,
                                     Optional<Map<String, Tag>> tags,
                                     Optional<String> structureVersionId,
                                     Optional<String> reference,
                                     Optional<Map<String, String>> parameters,
                                     String nodeId,
                                     Optional<String> parentId) throws GroundException {

        Node node = Node.retrieveFromDatabase(connection, Node.idToName(nodeId));

        String id = IdGenerator.generateId(nodeId);

        // add the id of the version to the tag
        tags = tags.map ( tagsMap ->
                                  tagsMap.values().stream().collect(Collectors.toMap(Tag::getKey, tag -> new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType())))
        );

        RichVersion.insertIntoDatabase(connection, id, tags, structureVersionId, reference, parameters);

        List<DbDataContainer> insertions = new ArrayList<>();
        insertions.add(new DbDataContainer("id", Type.STRING, id));
        insertions.add(new DbDataContainer("node_id", Type.STRING, nodeId));

        connection.insert("NodeVersions", insertions);

        node.update(connection, id, parentId);

        LOGGER.info("Created node version " + id + " in node " + nodeId + ".");

        return new NodeVersion(id, tags, structureVersionId, reference, parameters, nodeId);
    }

    public static NodeVersion retrieveFromDatabase(GroundDBConnection connection, String id) throws GroundException {
        RichVersion version = RichVersion.retrieveFromDatabase(connection, id);

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("id", Type.STRING, id));

        ResultSet resultSet = connection.equalitySelect("NodeVersions", DBClient.SELECT_STAR, predicates);
        String nodeId = DbUtils.getString(resultSet, 2);

        LOGGER.info("Retrieved node version " + id + " in node " + nodeId + ".");

        return new NodeVersion(id, version.getTags(), version.getStructureVersionId(), version.getReference(), version.getParameters(), nodeId);
    }
}
