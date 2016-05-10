package edu.berkeley.ground.api.models.postgres;

import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.NodeFactory;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.api.versions.postgres.PostgresItemFactory;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class PostgresNodeFactory extends NodeFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresNodeFactory.class);
    private PostgresClient dbClient;

    private PostgresItemFactory itemFactory;

    public PostgresNodeFactory(PostgresItemFactory itemFactory, PostgresClient dbClient) {
        this.dbClient = dbClient;
        this.itemFactory = itemFactory;
    }

    public Node create(String name) throws GroundException {
        GroundDBConnection connection = this.dbClient.getConnection();
        String uniqueId = "Nodes." + name;

        this.itemFactory.insertIntoDatabase(connection, uniqueId);

        List<DbDataContainer> insertions = new ArrayList<>();
        insertions.add(new DbDataContainer("name", Type.STRING, name));
        insertions.add(new DbDataContainer("item_id", Type.STRING, uniqueId));

        connection.insert("Nodes", insertions);

        connection.commit();
        LOGGER.info("Created node " + name + ".");

        return NodeFactory.construct(uniqueId, name);
    }

    public Node retrieveFromDatabase(String name) throws GroundException {
        GroundDBConnection connection = this.dbClient.getConnection();
        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("name", Type.STRING, name));

        ResultSet resultSet = connection.equalitySelect("Nodes", DBClient.SELECT_STAR, predicates);
        String id = DbUtils.getString(resultSet, 1);

        connection.commit();
        LOGGER.info("Retrieved node " + name + ".");

        return NodeFactory.construct(id, name);
    }

    public void update(GroundDBConnection connection, String itemId, String childId, Optional<String> parent) throws GroundException {
        this.itemFactory.update(connection, itemId, childId, parent);
    }
}
