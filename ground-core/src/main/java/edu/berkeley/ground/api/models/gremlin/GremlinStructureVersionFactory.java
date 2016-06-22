package edu.berkeley.ground.api.models.gremlin;

import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.StructureVersionFactory;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.GremlinClient;
import edu.berkeley.ground.db.GremlinClient.GremlinConnection;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class GremlinStructureVersionFactory extends StructureVersionFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinStructureVersionFactory.class);
    private GremlinClient dbClient;

    private GremlinStructureFactory structureFactory;

    public GremlinStructureVersionFactory(GremlinStructureFactory structureFactory, GremlinClient dbClient) {
        this.dbClient = dbClient;
        this.structureFactory = structureFactory;
    }

    public StructureVersion create(String structureId,
                                   Map<String, Type> attributes,
                                   Optional<String> parentId) throws GroundException {

        GremlinConnection connection = this.dbClient.getConnection();
        String id = IdGenerator.generateId(structureId);

        List<DbDataContainer> insertions = new ArrayList<>();
        insertions.add(new DbDataContainer("id", Type.STRING, id));
        insertions.add(new DbDataContainer("structure_id", Type.STRING, structureId));

        Vertex versionVertex = connection.addVertex("StructureVersion", insertions);

        for (String key : attributes.keySet()) {
            List<DbDataContainer> itemInsertions = new ArrayList<>();
            itemInsertions.add(new DbDataContainer("svid", Type.STRING, id));
            itemInsertions.add(new DbDataContainer("skey", Type.STRING, key));
            itemInsertions.add(new DbDataContainer("type", Type.STRING, attributes.get(key).toString()));

            Vertex itemVertex = connection.addVertex("StructureVersionItem", itemInsertions);
            connection.addEdge("StructureVersionItemConnection", versionVertex, itemVertex, new ArrayList<>());
        }

        this.structureFactory.update(connection, structureId, id, parentId);

        connection.commit();
        LOGGER.info("Created structure version " + id + " in structure " + structureId + ".");

        return StructureVersionFactory.construct(id, structureId, attributes);
    }

    public StructureVersion retrieveFromDatabase(String id) throws GroundException {
        GremlinConnection connection = this.dbClient.getConnection();

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("id", Type.STRING, id));
        Vertex versionVertex = connection.getVertex(predicates);

        List<Vertex> adjacentVetices = connection.getAdjacentVerticesByEdgeLabel(versionVertex, "StructureVersionItemConnection");
        Map<String, Type> attributes = new HashMap<>();

        for(Vertex titanVertex : adjacentVetices) {
            attributes.put(titanVertex.property("skey").value().toString(), Type.fromString(titanVertex.property("type").value().toString()));
        }

        String structureId = versionVertex.property("structure_id").toString();

        connection.commit();
        LOGGER.info("Retrieved structure version " + id + " in structure " + structureId + ".");

        return StructureVersionFactory.construct(id, structureId, attributes);
    }
}
