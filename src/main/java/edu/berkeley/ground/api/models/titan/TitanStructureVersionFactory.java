package edu.berkeley.ground.api.models.titan;

import com.thinkaurelius.titan.core.TitanVertex;
import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.StructureVersionFactory;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.TitanClient;
import edu.berkeley.ground.db.TitanClient.TitanConnection;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TitanStructureVersionFactory extends StructureVersionFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(TitanStructureVersionFactory.class);
    private TitanClient dbClient;

    private TitanStructureFactory structureFactory;

    public TitanStructureVersionFactory(TitanStructureFactory structureFactory, TitanClient dbClient) {
        this.dbClient = dbClient;
        this.structureFactory = structureFactory;
    }

    public StructureVersion create(String structureId,
                                   Map<String, Type> attributes,
                                   Optional<String> parentId) throws GroundException {

        TitanConnection connection = this.dbClient.getConnection();
        String id = IdGenerator.generateId(structureId);

        List<DbDataContainer> insertions = new ArrayList<>();
        insertions.add(new DbDataContainer("id", Type.STRING, id));
        insertions.add(new DbDataContainer("structure_id", Type.STRING, structureId));

        TitanVertex versionVertex = connection.addVertex("StructureVersion", insertions);

        for (String key : attributes.keySet()) {
            List<DbDataContainer> itemInsertions = new ArrayList<>();
            itemInsertions.add(new DbDataContainer("svid", Type.STRING, id));
            itemInsertions.add(new DbDataContainer("skey", Type.STRING, key));
            itemInsertions.add(new DbDataContainer("type", Type.STRING, attributes.get(key).toString()));

            TitanVertex itemVertex = connection.addVertex("StructureVersionItem", itemInsertions);
            connection.addEdge("StructureVersionItemConnection", versionVertex, itemVertex, new ArrayList<>());
        }

        this.structureFactory.update(connection, structureId, id, parentId);

        connection.commit();
        LOGGER.info("Created structure version " + id + " in structure " + structureId + ".");

        return StructureVersionFactory.construct(id, structureId, attributes);
    }

    public StructureVersion retrieveFromDatabase(String id) throws GroundException {
        TitanConnection connection = this.dbClient.getConnection();

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("id", Type.STRING, id));
        TitanVertex versionVertex = connection.getVertex(predicates);

        List<TitanVertex> adjacentVetices = connection.getAdjacentVerticesByEdgeLabel(versionVertex, "StructureVersionItemConnection");
        Map<String, Type> attributes = new HashMap<>();

        for(TitanVertex titanVertex : adjacentVetices) {
            attributes.put(titanVertex.property("skey").value().toString(), Type.fromString(titanVertex.property("type").value().toString()));
        }

        String structureId = versionVertex.property("structure_id").toString();

        connection.commit();
        LOGGER.info("Retrieved structure version " + id + " in structure " + structureId + ".");

        return StructureVersionFactory.construct(id, structureId, attributes);
    }
}
