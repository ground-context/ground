package edu.berkeley.ground.api.models;

import com.fasterxml.jackson.annotation.JsonCreator;
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

public class Edge extends Item<EdgeVersion> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Edge.class);

    // the name of this Edge
    private String name;

    @JsonCreator
    protected Edge(@JsonProperty("id") String id,
                   @JsonProperty("name") String name) {
        super(id);

        this.name = name;
    }

    @JsonProperty
    public String getName() {
        return this.name;
    }

    /* FACTORY METHODS */
    public static Edge create(GroundDBConnection connection, String name) throws GroundException {
        String uniqueId = "Edges." + name;

        Item.insertIntoDatabase(connection, uniqueId);

        List<DbDataContainer> insertions = new ArrayList<>();
        insertions.add(new DbDataContainer("name", Type.STRING, name));
        insertions.add(new DbDataContainer("item_id", Type.STRING, uniqueId));

        connection.insert("Edges", insertions);

        LOGGER.info("Created edge " + name + ".");

        return new Edge(uniqueId, name);
    }

    public static Edge retrieveFromDatabase(GroundDBConnection connection, String name) throws GroundException {
        List<DbDataContainer> predicates = new ArrayList<>();

        predicates.add(new DbDataContainer("name", Type.STRING, name));

        ResultSet resultSet = connection.equalitySelect("Edges", DBClient.SELECT_STAR, predicates);
        String id = DbUtils.getString(resultSet, 1);

        LOGGER.info("Retrieved edge " + name + ".");

        return new Edge(id, name);
    }

    public static String idToName(String id) {
        return id.substring(6);
    }
}
