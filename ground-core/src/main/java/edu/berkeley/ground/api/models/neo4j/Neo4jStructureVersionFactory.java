package edu.berkeley.ground.api.models.neo4j;

import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.StructureVersionFactory;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.db.Neo4jClient.Neo4jConnection;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Neo4jStructureVersionFactory extends StructureVersionFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jStructureVersionFactory.class);
    private Neo4jClient dbClient;
    private Neo4jStructureFactory structureFactory;

    public Neo4jStructureVersionFactory(Neo4jClient dbClient, Neo4jStructureFactory structureFactory) {
        this.dbClient = dbClient;
        this.structureFactory = structureFactory;
    }

    public StructureVersion create(String structureId, Map<String, Type> attributes, Optional<String> parentId) throws GroundException {
        Neo4jConnection connection = this.dbClient.getConnection();

        String id = IdGenerator.generateId(structureId);

        List<DbDataContainer> insertions = new ArrayList<>();
        insertions.add(new DbDataContainer("id", Type.STRING, id));
        insertions.add(new DbDataContainer("structure_id", Type.STRING, structureId));

        connection.addVertex("StructureVersion", insertions);

        for (String key : attributes.keySet()) {
            List<DbDataContainer> itemInsertions = new ArrayList<>();
            itemInsertions.add(new DbDataContainer("svid", Type.STRING, id));
            itemInsertions.add(new DbDataContainer("skey", Type.STRING, key));
            itemInsertions.add(new DbDataContainer("stype", Type.STRING, attributes.get(key).toString()));

            connection.addVertexAndEdge("StructureVersionItem", itemInsertions, "StructureVersionItemConnection", id, new ArrayList<>());
        }

        this.structureFactory.update(connection, structureId, id, parentId);

        connection.commit();
        LOGGER.info("Created structure version " + id + " in structure " + structureId + ".");

        return StructureVersionFactory.construct(id, structureId, attributes);
    }

    public StructureVersion retrieveFromDatabase(String id) throws GroundException {
        Neo4jConnection connection = this.dbClient.getConnection();

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("id", Type.STRING, id));

        String structureId = connection.getVertex(predicates).get("structure_id").toString();
        List<String> returnFields = new ArrayList<>();
        returnFields.add("svid");
        returnFields.add("skey");
        returnFields.add("stype");

        List<Record> edges = connection.getAdjacentVerticesByEdgeLabel("StructureVersionItemConnection", id, returnFields);
        Map<String, Type> attributes = new HashMap<>();


        for (Record record : edges) {
            attributes.put(Neo4jClient.getStringFromValue((StringValue) record.get("skey")), Type.fromString( Neo4jClient.getStringFromValue((StringValue) record.get("stype"))));
        }

        return StructureVersionFactory.construct(id, structureId, attributes);
    }

}
