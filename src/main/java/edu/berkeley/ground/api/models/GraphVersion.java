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

    /* FACTORY METHODS */

    public static GraphVersion create(GroundDBConnection connection,
                                      Optional<Map<String, Tag>> tags,
                                      Optional<String> structureVersionId,
                                      Optional<String> reference,
                                      Optional<Map<String, String>> parameters,
                                      String graphId,
                                      List<String> edgeVersionIds,
                                      Optional<String> parentId) throws GroundException{

        Graph graph = Graph.retrieveFromDatabase(connection, Graph.idToName(graphId));

        String id = IdGenerator.generateId(graphId);

        tags = tags.map ( tagsMap ->
                                  tagsMap.values().stream().collect(Collectors.toMap(Tag::getKey, tag -> new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType())))
        );

        RichVersion.insertIntoDatabase(connection, id, tags, structureVersionId, reference, parameters);

        List<DbDataContainer> insertions = new ArrayList<>();
        insertions.add(new DbDataContainer("id", Type.STRING, id));
        insertions.add(new DbDataContainer("graph_id", Type.STRING, graphId));

        connection.insert("GraphVersions", insertions);

        for (String edgeVersionId : edgeVersionIds) {
            List<DbDataContainer> edgeInsertion = new ArrayList<>();
            edgeInsertion.add(new DbDataContainer("gvid", Type.STRING, id));
            edgeInsertion.add(new DbDataContainer("evid", Type.STRING, edgeVersionId));

            connection.insert("GraphVersionEdges", edgeInsertion);
        }

        graph.update(connection, id, parentId);

        LOGGER.info("Created graph version " + id + " in graph " + graphId + ".");

        return new GraphVersion(id, tags, structureVersionId, reference, parameters, graphId, edgeVersionIds);
    }

    public static GraphVersion retrieveFromDatabase(GroundDBConnection connection, String id) throws GroundException {
        RichVersion version = RichVersion.retrieveFromDatabase(connection, id);

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("id", Type.STRING, id));

        List<DbDataContainer> edgePredicate = new ArrayList<>();
        edgePredicate.add(new DbDataContainer("gvid", Type.STRING, id));

        ResultSet resultSet = connection.equalitySelect("GraphVersions", DBClient.SELECT_STAR, predicates);
        String graphId = DbUtils.getString(resultSet, 2);

        ResultSet edgeSet = connection.equalitySelect("GraphVersionEdges", DBClient.SELECT_STAR, edgePredicate);
        List<String> edgeVersionIds = DbUtils.getAllStrings(edgeSet, 2);

        LOGGER.info("Retrieved graph version " + id + " in graph " + graphId + ".");

        return new GraphVersion(id, version.getTags(), version.getStructureVersionId(), version.getReference(), version.getParameters(), graphId, edgeVersionIds);
    }
}
