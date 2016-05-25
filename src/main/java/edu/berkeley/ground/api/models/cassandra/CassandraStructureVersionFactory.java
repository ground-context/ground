package edu.berkeley.ground.api.models.cassandra;

import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.StructureVersionFactory;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.api.versions.cassandra.CassandraVersionFactory;
import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.db.CassandraClient.CassandraConnection;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.QueryResults;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CassandraStructureVersionFactory extends StructureVersionFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraStructureVersionFactory.class);
    private CassandraClient dbClient;

    private CassandraStructureFactory structureFactory;
    private CassandraVersionFactory versionFactory;

    public CassandraStructureVersionFactory(CassandraStructureFactory structureFactory, CassandraVersionFactory versionFactory, CassandraClient dbClient) {
        this.dbClient = dbClient;
        this.structureFactory = structureFactory;
        this.versionFactory = versionFactory;
    }

    public StructureVersion create(String structureId,
                                   Map<String, Type> attributes,
                                   Optional<String> parentId) throws GroundException {

        CassandraConnection connection = this.dbClient.getConnection();
        String id = IdGenerator.generateId(structureId);

        this.versionFactory.insertIntoDatabase(connection, id);

        List<DbDataContainer> insertions = new ArrayList<>();
        insertions.add(new DbDataContainer("id", Type.STRING, id));
        insertions.add(new DbDataContainer("structure_id", Type.STRING, structureId));

        connection.insert("StructureVersions", insertions);

        for (String key : attributes.keySet()) {
            List<DbDataContainer> itemInsertions = new ArrayList<>();
            itemInsertions.add(new DbDataContainer("svid", Type.STRING, id));
            itemInsertions.add(new DbDataContainer("key", Type.STRING, key));
            itemInsertions.add(new DbDataContainer("type", Type.STRING, attributes.get(key).toString()));

            connection.insert("StructureVersionItems", itemInsertions);
        }

        this.structureFactory.update(connection, structureId, id, parentId);

        connection.commit();
        LOGGER.info("Created structure version " + id + " in structure " + structureId + ".");

        return StructureVersionFactory.construct(id, structureId, attributes);
    }

    public StructureVersion retrieveFromDatabase(String id) throws GroundException {
        CassandraConnection connection = this.dbClient.getConnection();

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("id", Type.STRING, id));
        QueryResults resultSet = connection.equalitySelect("StructureVersions", DBClient.SELECT_STAR, predicates);

        List<DbDataContainer> attributePredicates = new ArrayList<>();
        attributePredicates.add(new DbDataContainer("svid", Type.STRING, id));
        QueryResults attributesSet = connection.equalitySelect("StructureVersionItems", DBClient.SELECT_STAR, attributePredicates);

        Map<String, Type> attributes = new HashMap<>();

        do {
            attributes.put(attributesSet.getString(1), Type.fromString(attributesSet.getString(2)));
        } while (attributesSet.next());

        String structureId = resultSet.getString(1);

        connection.commit();
        LOGGER.info("Retrieved structure version " + id + " in structure " + structureId + ".");

        return StructureVersionFactory.construct(id, structureId, attributes);
    }
}
