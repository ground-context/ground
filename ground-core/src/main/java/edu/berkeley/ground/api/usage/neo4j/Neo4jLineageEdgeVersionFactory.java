package edu.berkeley.ground.api.usage.neo4j;

import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.models.neo4j.Neo4jRichVersionFactory;
import edu.berkeley.ground.api.usage.LineageEdgeVersion;
import edu.berkeley.ground.api.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.db.Neo4jClient.Neo4jConnection;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;
import org.neo4j.driver.v1.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class Neo4jLineageEdgeVersionFactory extends LineageEdgeVersionFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jLineageEdgeVersionFactory.class);
    private Neo4jClient dbClient;

    private Neo4jLineageEdgeFactory lineageEdgeFactory;
    private Neo4jRichVersionFactory richVersionFactory;

    public Neo4jLineageEdgeVersionFactory(Neo4jLineageEdgeFactory lineageEdgeFactory, Neo4jRichVersionFactory richVersionFactory, Neo4jClient dbClient) {
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
                                     Optional<String> parentId) throws GroundException {

        Neo4jConnection connection = this.dbClient.getConnection();

        try {
            String id = IdGenerator.generateId(lineageEdgeId);

            tags = tags.map(tagsMap ->
                                    tagsMap.values().stream().collect(Collectors.toMap(Tag::getKey, tag -> new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType())))
            );

            List<DbDataContainer> insertions = new ArrayList<>();
            insertions.add(new DbDataContainer("id", Type.STRING, id));
            insertions.add(new DbDataContainer("lineageedge_id", Type.STRING, lineageEdgeId));
            insertions.add(new DbDataContainer("endpoint_one", Type.STRING, fromId));
            insertions.add(new DbDataContainer("endpoint_two", Type.STRING, toId));

            connection.addVertex("LineageEdgeVersions", insertions);

            this.lineageEdgeFactory.update(connection, lineageEdgeId, id, parentId);
            this.richVersionFactory.insertIntoDatabase(connection, id, tags, structureVersionId, reference, parameters);

            connection.addEdge("LineageEdgeVersionConnection", fromId, id, new ArrayList<>());
            connection.addEdge("LineageEdgeVersionConnection", id, toId, new ArrayList<>());

            connection.commit();
            LOGGER.info("Created lineage edge version " + id + " in lineage edge " + lineageEdgeId + ".");

            return LineageEdgeVersionFactory.construct(id, tags, structureVersionId, reference, parameters, fromId, toId, lineageEdgeId);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }

    public LineageEdgeVersion retrieveFromDatabase(String id) throws GroundException {
        Neo4jConnection connection = this.dbClient.getConnection();

        try {
            RichVersion version = this.richVersionFactory.retrieveFromDatabase(connection, id);

            List<DbDataContainer> predicates = new ArrayList<>();
            predicates.add(new DbDataContainer("id", Type.STRING, id));

            Record versionRecord = connection.getVertex(predicates);
            String lineageEdgeId = versionRecord.get("lineageedge_id").toString();
            String fromId = versionRecord.get("endpoint_one").toString();
            String toId = versionRecord.get("endpoint_two").toString();

            connection.commit();
            LOGGER.info("Retrieved lineage edge version " + id + " in lineage edge " + lineageEdgeId + ".");

            return LineageEdgeVersionFactory.construct(id, version.getTags(), version.getStructureVersionId(), version.getReference(), version.getParameters(), fromId, toId, lineageEdgeId);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }
}
