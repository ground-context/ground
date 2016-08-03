package edu.berkeley.ground.api.models.neo4j;

import edu.berkeley.ground.api.models.EdgeVersion;
import edu.berkeley.ground.api.models.EdgeVersionFactory;
import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.Tag;
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

public class Neo4jEdgeVersionFactory extends EdgeVersionFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jEdgeVersionFactory.class);
    private Neo4jClient dbClient;

    private Neo4jEdgeFactory edgeFactory;
    private Neo4jRichVersionFactory richVersionFactory;

    public Neo4jEdgeVersionFactory(Neo4jEdgeFactory edgeFactory, Neo4jRichVersionFactory richVersionFactory, Neo4jClient dbClient) {
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

        Neo4jConnection connection = this.dbClient.getConnection();

        try {
            String id = IdGenerator.generateId(edgeId);

            tags = tags.map(tagsMap ->
                                    tagsMap.values().stream().collect(Collectors.toMap(Tag::getKey, tag -> new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType())))
            );


            List<DbDataContainer> insertions = new ArrayList<>();
            insertions.add(new DbDataContainer("id", Type.STRING, id));
            insertions.add(new DbDataContainer("edge_id", Type.STRING, edgeId));
            insertions.add(new DbDataContainer("endpoint_one", Type.STRING, fromId));
            insertions.add(new DbDataContainer("endpoint_two", Type.STRING, toId));

            connection.addVertex("EdgeVersion", insertions);
            this.richVersionFactory.insertIntoDatabase(connection, id, tags, structureVersionId, reference, parameters);

            connection.addEdge("EdgeVersionConnection", fromId, id, new ArrayList<>());
            connection.addEdge("EdgeVersionConnection", id, toId, new ArrayList<>());

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
        Neo4jConnection connection = this.dbClient.getConnection();

        try {
            RichVersion version = this.richVersionFactory.retrieveFromDatabase(connection, id);

            List<DbDataContainer> predicates = new ArrayList<>();
            predicates.add(new DbDataContainer("id", Type.STRING, id));

            Record versionRecord = connection.getVertex(predicates);
            String edgeId = versionRecord.get("edge_id").toString();
            String fromId = versionRecord.get("endpoint_one").toString();
            String toId = versionRecord.get("endpoint_two").toString();

            connection.commit();
            LOGGER.info("Retrieved edge version " + id + " in edge " + edgeId + ".");

            return EdgeVersionFactory.construct(id, version.getTags(), version.getStructureVersionId(), version.getReference(), version.getParameters(), edgeId, fromId, toId);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }
}
