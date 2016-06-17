package edu.berkeley.ground.api.models.postgres;

import edu.berkeley.ground.api.models.Graph;
import edu.berkeley.ground.api.models.GraphFactory;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.api.versions.postgres.PostgresItemFactory;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.db.PostgresClient.PostgresConnection;
import edu.berkeley.ground.db.QueryResults;
import edu.berkeley.ground.exceptions.GroundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class PostgresGraphFactory extends GraphFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresGraphFactory.class);
    private PostgresClient dbClient;

    private PostgresItemFactory itemFactory;

    public PostgresGraphFactory(PostgresItemFactory itemFactory, PostgresClient dbClient) {
        this.dbClient = dbClient;
        this.itemFactory = itemFactory;
    }

    public Graph create(String name) throws GroundException {
        PostgresConnection connection = this.dbClient.getConnection();

        try {
            String uniqueId = "Graphs." + name;
            this.itemFactory.insertIntoDatabase(connection, uniqueId);

            List<DbDataContainer> insertions = new ArrayList<>();
            insertions.add(new DbDataContainer("name", Type.STRING, name));
            insertions.add(new DbDataContainer("item_id", Type.STRING, uniqueId));

            connection.insert("Graphs", insertions);

            connection.commit();
            LOGGER.info("Created graph " + name + ".");

            return GraphFactory.construct(uniqueId, name);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }

    public Graph retrieveFromDatabase(String name) throws GroundException {
        PostgresConnection connection = this.dbClient.getConnection();

        try {
            List<DbDataContainer> predicates = new ArrayList<>();
            predicates.add(new DbDataContainer("name", Type.STRING, name));

            QueryResults resultSet = connection.equalitySelect("Graphs", DBClient.SELECT_STAR, predicates);
            String id = resultSet.getString(1);

            connection.commit();
            LOGGER.info("Retrieved graph " + name + ".");

            return GraphFactory.construct(id, name);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }

    public void update(GroundDBConnection connection, String itemId, String childId, Optional<String> parent) throws GroundException {
        this.itemFactory.update(connection, itemId, childId, parent);
    }
}
