package edu.berkeley.ground.api.models.gremlin;

import edu.berkeley.ground.api.models.EdgeVersion;
import edu.berkeley.ground.api.models.EdgeVersionFactory;
import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.GremlinClient;
import edu.berkeley.ground.db.GremlinClient.GremlinConnection;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class GremlinEdgeVersionFactory extends EdgeVersionFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinEdgeVersionFactory.class);
    private GremlinClient dbClient;

    private GremlinEdgeFactory edgeFactory;
    private GremlinRichVersionFactory richVersionFactory;

    public GremlinEdgeVersionFactory(GremlinEdgeFactory edgeFactory, GremlinRichVersionFactory richVersionFactory, GremlinClient dbClient) {
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

        GremlinConnection connection = this.dbClient.getConnection();

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

            Vertex vertex = connection.addVertex("EdgeVersion", insertions);
            this.richVersionFactory.insertIntoDatabase(connection, id, tags, structureVersionId, reference, parameters);

            List<DbDataContainer> predicates = new ArrayList<>();
            predicates.add(new DbDataContainer("id", Type.STRING, fromId));
            Vertex fromVertex = connection.getVertex(predicates);

            predicates.clear();
            predicates.add(new DbDataContainer("id", Type.STRING, toId));
            Vertex toVertex = connection.getVertex(predicates);

            predicates.clear();
            connection.addEdge("EdgeVersionConnection", fromVertex, vertex, predicates);
            connection.addEdge("EdgeVersionConnection", vertex, toVertex, predicates);

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
        GremlinConnection connection = this.dbClient.getConnection();

        try {
            RichVersion version = this.richVersionFactory.retrieveFromDatabase(connection, id);

            List<DbDataContainer> predicates = new ArrayList<>();
            predicates.add(new DbDataContainer("id", Type.STRING, id));

            Vertex versionVertex = connection.getVertex(predicates);
            String edgeId = versionVertex.property("edge_id").value().toString();
            String fromId = versionVertex.property("endpoint_one").value().toString();
            String toId = versionVertex.property("endpoint_two").value().toString();

            connection.commit();
            LOGGER.info("Retrieved edge version " + id + " in edge " + edgeId + ".");

            return EdgeVersionFactory.construct(id, version.getTags(), version.getStructureVersionId(), version.getReference(), version.getParameters(), edgeId, fromId, toId);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }
}
