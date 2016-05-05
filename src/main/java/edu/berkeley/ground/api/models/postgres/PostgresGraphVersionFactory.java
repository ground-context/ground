package edu.berkeley.ground.api.models.postgres;

import edu.berkeley.ground.api.models.GraphVersion;
import edu.berkeley.ground.api.models.GraphVersionFactory;
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

public class PostgresGraphVersionFactory extends GraphVersionFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresGraphVersionFactory.class);

    private PostgresGraphFactory graphFactory;
    private PostgresRichVersionFactory richVersionFactory;

    public PostgresGraphVersionFactory(PostgresGraphFactory graphFactory, PostgresRichVersionFactory richVersionFactory) {
        this.graphFactory = graphFactory;
        this.richVersionFactory = richVersionFactory;
    }

    public GraphVersion create(GroundDBConnection connection,
                               Optional<Map<String, Tag>> tags,
                               Optional<String> structureVersionId,
                               Optional<String> reference,
                               Optional<Map<String, String>> parameters,
                               String graphId,
                               List<String> edgeVersionIds,
                               Optional<String> parentId) throws GroundException {

        String id = IdGenerator.generateId(graphId);

        tags = tags.map ( tagsMap ->
                                  tagsMap.values().stream().collect(Collectors.toMap(Tag::getKey, tag -> new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType())))
        );

        this.richVersionFactory.insertIntoDatabase(connection, id, tags, structureVersionId, reference, parameters);

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

        this.graphFactory.update(connection, graphId, id, parentId);

        LOGGER.info("Created graph version " + id + " in graph " + graphId + ".");

        return GraphVersionFactory.construct(id, tags, structureVersionId, reference, parameters, graphId, edgeVersionIds);
    }

    public GraphVersion retrieveFromDatabase(GroundDBConnection connection, String id) throws GroundException {
        RichVersion version = this.richVersionFactory.retrieveFromDatabase(connection, id);

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("id", Type.STRING, id));

        List<DbDataContainer> edgePredicate = new ArrayList<>();
        edgePredicate.add(new DbDataContainer("gvid", Type.STRING, id));

        ResultSet resultSet = connection.equalitySelect("GraphVersions", DBClient.SELECT_STAR, predicates);
        String graphId = DbUtils.getString(resultSet, 2);

        ResultSet edgeSet = connection.equalitySelect("GraphVersionEdges", DBClient.SELECT_STAR, edgePredicate);
        List<String> edgeVersionIds = DbUtils.getAllStrings(edgeSet, 2);

        LOGGER.info("Retrieved graph version " + id + " in graph " + graphId + ".");

        return GraphVersionFactory.construct(id, version.getTags(), version.getStructureVersionId(), version.getReference(), version.getParameters(), graphId, edgeVersionIds);
    }
}
