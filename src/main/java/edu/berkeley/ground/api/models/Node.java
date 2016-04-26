package edu.berkeley.ground.api.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.berkeley.ground.api.versions.Item;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class Node extends Item<NodeVersion> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Node.class);

    // the name of this Node
    private String name;

    protected Node(String id, String name) {
        super(id);

        this.name = name;
    }

    @JsonProperty
    public String getName() {
        return this.name;
    }

    /* FACTORY METHODS */
    public static Node create(GroundDBConnection connection, String name) throws GroundException {
        String uniqueId = "Nodes." + name;

        Item.insertIntoDatabase(connection, uniqueId);

        List<DbDataContainer> insertions = new ArrayList<>();
        insertions.add(new DbDataContainer("name", Type.STRING, name));
        insertions.add(new DbDataContainer("item_id", Type.STRING, uniqueId));

        connection.insert("Nodes", insertions);

        LOGGER.info("Created node " + name + ".");

        return new Node(uniqueId, name);
    }

    public static Node retrieveFromDatabase(GroundDBConnection connection, String name) throws GroundException {
        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("name", Type.STRING, name));

        ResultSet resultSet = connection.equalitySelect("Nodes", DBClient.SELECT_STAR, predicates);
        String id = DbUtils.getString(resultSet, 1);

        LOGGER.info("Retrieved node " + name + ".");

        return new Node(id, name);
    }

    public static String idToName(String id) {
        return id.substring(6);
    }
}
