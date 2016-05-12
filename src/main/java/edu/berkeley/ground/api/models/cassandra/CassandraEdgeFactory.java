package edu.berkeley.ground.api.models.cassandra;

import edu.berkeley.ground.api.models.Edge;
import edu.berkeley.ground.api.models.EdgeFactory;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.api.versions.cassandra.CassandraItemFactory;
import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.QueryResults;
import edu.berkeley.ground.exceptions.GroundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CassandraEdgeFactory extends EdgeFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraEdgeFactory.class);
    private CassandraClient dbClient;

    private CassandraItemFactory itemFactory;

    public CassandraEdgeFactory(CassandraItemFactory itemFactory, CassandraClient dbClient) {
        this.dbClient = dbClient;
        this.itemFactory = itemFactory;
    }

    public Edge create(String name) throws GroundException {
        GroundDBConnection connection = dbClient.getConnection();

        try {
            String uniqueId = "Edges." + name;

            this.itemFactory.insertIntoDatabase(connection, uniqueId);

            List<DbDataContainer> insertions = new ArrayList<>();
            insertions.add(new DbDataContainer("name", Type.STRING, name));
            insertions.add(new DbDataContainer("item_id", Type.STRING, uniqueId));

            connection.insert("Edges", insertions);

            connection.commit();
            LOGGER.info("Created edge " + name + ".");
            return EdgeFactory.construct(uniqueId, name);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }

    public Edge retrieveFromDatabase(String name) throws GroundException {
        GroundDBConnection connection = dbClient.getConnection();

        try {
            List<DbDataContainer> predicates = new ArrayList<>();

            predicates.add(new DbDataContainer("name", Type.STRING, name));

            QueryResults resultSet = connection.equalitySelect("Edges", DBClient.SELECT_STAR, predicates);
            String id = resultSet.getString(0);

            connection.commit();
            LOGGER.info("Retrieved edge " + name + ".");

            return EdgeFactory.construct(id, name);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }

    public void update(GroundDBConnection connection, String itemId, String childId, Optional<String> parent) throws GroundException {
        this.itemFactory.update(connection, itemId, childId, parent);
    }
}
