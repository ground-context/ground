package edu.berkeley.ground.api.models.postgres;

import edu.berkeley.ground.api.models.GraphVersion;
import edu.berkeley.ground.api.models.GraphVersionFactory;
import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.db.QueryResults;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class PostgresGraphVersionFactory extends GraphVersionFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresGraphVersionFactory.class);
    private PostgresClient dbClient;

    private PostgresGraphFactory graphFactory;
    private PostgresRichVersionFactory richVersionFactory;

    public PostgresGraphVersionFactory(PostgresGraphFactory graphFactory, PostgresRichVersionFactory richVersionFactory, PostgresClient dbClient) {
        this.dbClient = dbClient;
        this.graphFactory = graphFactory;
        this.richVersionFactory = richVersionFactory;
    }

    public GraphVersion create(Optional<Map<String, Tag>> tags,
                               Optional<String> structureVersionId,
                               Optional<String> reference,
                               Optional<Map<String, String>> parameters,
                               String graphId,
                               List<String> edgeVersionIds,
                               Optional<String> parentId) throws GroundException {

        GroundDBConnection connection = this.dbClient.getConnection();

        try {
            String id = IdGenerator.generateId(graphId);

            tags = tags.map(tagsMap ->
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

            connection.commit();
            LOGGER.info("Created graph version " + id + " in graph " + graphId + ".");

            return GraphVersionFactory.construct(id, tags, structureVersionId, reference, parameters, graphId, edgeVersionIds);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }

    public GraphVersion retrieveFromDatabase(String id) throws GroundException {
        GroundDBConnection connection = this.dbClient.getConnection();

        try {
            RichVersion version = this.richVersionFactory.retrieveFromDatabase(connection, id);

            List<DbDataContainer> predicates = new ArrayList<>();
            predicates.add(new DbDataContainer("id", Type.STRING, id));

            List<DbDataContainer> edgePredicate = new ArrayList<>();
            edgePredicate.add(new DbDataContainer("gvid", Type.STRING, id));

            QueryResults resultSet = connection.equalitySelect("GraphVersions", DBClient.SELECT_STAR, predicates);
            String graphId = resultSet.getString(2);

            QueryResults edgeSet = connection.equalitySelect("GraphVersionEdges", DBClient.SELECT_STAR, edgePredicate);
            List<String> edgeVersionIds = edgeSet.getStringList();

            connection.commit();
            LOGGER.info("Retrieved graph version " + id + " in graph " + graphId + ".");

            return GraphVersionFactory.construct(id, version.getTags(), version.getStructureVersionId(), version.getReference(), version.getParameters(), graphId, edgeVersionIds);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }
}
