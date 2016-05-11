package edu.berkeley.ground.api.models.postgres;

import edu.berkeley.ground.api.models.*;
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

public class PostgresEdgeVersionFactory extends EdgeVersionFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresEdgeVersionFactory.class);
    private PostgresClient dbClient;

    private PostgresEdgeFactory edgeFactory;
    private PostgresRichVersionFactory richVersionFactory;

    public PostgresEdgeVersionFactory(PostgresEdgeFactory edgeFactory, PostgresRichVersionFactory richVersionFactory, PostgresClient dbClient) {
        this.dbClient = dbClient;
        this.edgeFactory = edgeFactory;
        this.richVersionFactory = richVersionFactory;
    }

    public EdgeVersion create(Optional<Map<String, Tag>> tags,
                              Optional<String> structureVersionId,
                              Optional<String> reference,
                              Optional<Map<String, String>> parameters,
                              String edgeId,
                              String fromId,
                              String toId,
                              Optional<String> parentId) throws GroundException {

        GroundDBConnection connection = this.dbClient.getConnection();

        try {
            String id = IdGenerator.generateId(edgeId);

            tags = tags.map(tagsMap ->
                                    tagsMap.values().stream().collect(Collectors.toMap(Tag::getKey, tag -> new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType())))
            );

            this.richVersionFactory.insertIntoDatabase(connection, id, tags, structureVersionId, reference, parameters);

            List<DbDataContainer> insertions = new ArrayList<>();
            insertions.add(new DbDataContainer("id", Type.STRING, id));
            insertions.add(new DbDataContainer("edge_id", Type.STRING, edgeId));
            insertions.add(new DbDataContainer("endpoint_one", Type.STRING, fromId));
            insertions.add(new DbDataContainer("endpoint_two", Type.STRING, toId));

            connection.insert("EdgeVersions", insertions);

            this.edgeFactory.update(connection, edgeId, id, parentId);

            connection.commit();
            LOGGER.info("Created edge version " + id + " in edge " + edgeId + ".");

            return EdgeVersionFactory.construct(id, tags, structureVersionId, reference, parameters, edgeId, fromId, toId);
        } catch (GroundException e) {
            connection.abort();
            throw e;
        }
    }

    public EdgeVersion retrieveFromDatabase(String id) throws GroundException {
        GroundDBConnection connection = this.dbClient.getConnection();

        try {
            RichVersion version = this.richVersionFactory.retrieveFromDatabase(connection, id);

            List<DbDataContainer> predicates = new ArrayList<>();
            predicates.add(new DbDataContainer("id", Type.STRING, id));

            QueryResults resultSet = connection.equalitySelect("EdgeVersions", DBClient.SELECT_STAR, predicates);
            String edgeId = resultSet.getString(2);
            String fromId = resultSet.getString(3);
            String toId = resultSet.getString(4);

            connection.commit();
            LOGGER.info("Retrieved edge version " + id + " in edge " + edgeId + ".");

            return EdgeVersionFactory.construct(id, version.getTags(), version.getStructureVersionId(), version.getReference(), version.getParameters(), edgeId, fromId, toId);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }
}
