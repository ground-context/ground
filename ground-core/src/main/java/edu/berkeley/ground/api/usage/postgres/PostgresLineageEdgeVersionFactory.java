package edu.berkeley.ground.api.usage.postgres;

import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.models.postgres.PostgresRichVersionFactory;
import edu.berkeley.ground.api.usage.LineageEdgeVersion;
import edu.berkeley.ground.api.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.db.PostgresClient.PostgresConnection;
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

public class PostgresLineageEdgeVersionFactory extends LineageEdgeVersionFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresLineageEdgeVersionFactory.class);
    private PostgresClient dbClient;

    private PostgresLineageEdgeFactory lineageEdgeFactory;
    private PostgresRichVersionFactory richVersionFactory;

    public PostgresLineageEdgeVersionFactory(PostgresLineageEdgeFactory lineageEdgeFactory, PostgresRichVersionFactory richVersionFactory, PostgresClient dbClient) {
        this.dbClient = dbClient;
        this.lineageEdgeFactory = lineageEdgeFactory;
        this.richVersionFactory = richVersionFactory;
    }


    public LineageEdgeVersion create(Optional<Map<String, Tag>> tags,
                                     Optional<String> structureVersionId,
                                     Optional<String> reference,
                                     Optional<Map<String, String>> parameters,
                                     String fromId,
                                     String toId,
                                     String lineageEdgeId,
                                     List<String> parentIds) throws GroundException {

        PostgresConnection connection = this.dbClient.getConnection();

        try {
            String id = IdGenerator.generateId(lineageEdgeId);

            tags = tags.map(tagsMap ->
                                    tagsMap.values().stream().collect(Collectors.toMap(Tag::getKey, tag -> new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType())))
            );

            this.richVersionFactory.insertIntoDatabase(connection, id, tags, structureVersionId, reference, parameters);

            List<DbDataContainer> insertions = new ArrayList<>();
            insertions.add(new DbDataContainer("id", GroundType.STRING, id));
            insertions.add(new DbDataContainer("lineageedge_id", GroundType.STRING, lineageEdgeId));
            insertions.add(new DbDataContainer("endpoint_one", GroundType.STRING, fromId));
            insertions.add(new DbDataContainer("endpoint_two", GroundType.STRING, toId));

            connection.insert("LineageEdgeVersions", insertions);

            this.lineageEdgeFactory.update(connection, lineageEdgeId, id, parentIds);

            connection.commit();
            LOGGER.info("Created lineage edge version " + id + " in lineage edge " + lineageEdgeId + ".");

            return LineageEdgeVersionFactory.construct(id, tags, structureVersionId, reference, parameters, fromId, toId, lineageEdgeId);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }

    public LineageEdgeVersion retrieveFromDatabase(String id) throws GroundException {
        PostgresConnection connection = this.dbClient.getConnection();

        try {
            RichVersion version = this.richVersionFactory.retrieveFromDatabase(connection, id);

            List<DbDataContainer> predicates = new ArrayList<>();
            predicates.add(new DbDataContainer("id", GroundType.STRING, id));

            QueryResults resultSet = connection.equalitySelect("LineageEdgeVersions", DBClient.SELECT_STAR, predicates);

            String lineageEdgeId = resultSet.getString(2);
            String fromId = resultSet.getString(3);
            String toId = resultSet.getString(4);

            connection.commit();
            LOGGER.info("Retrieved lineage edge version " + id + " in lineage edge " + lineageEdgeId + ".");

            return LineageEdgeVersionFactory.construct(id, version.getTags(), version.getStructureVersionId(), version.getReference(), version.getParameters(), fromId, toId, lineageEdgeId);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }
}
