package edu.berkeley.ground.api.versions;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.ArrayList;
import java.util.List;

public class Version {
    private String id;

    protected Version(@JsonProperty String id) {
        this.id = id;
    }

    @JsonProperty
    public String getId() {
        return this.id;
    }

    /* FACTORY METHODS */
    public static void insertIntoDatabase(GroundDBConnection connection, String id) throws GroundException {
        List<DbDataContainer> insertions = new ArrayList<>();
        insertions.add(new DbDataContainer("id", Type.STRING, id));

        connection.insert("Versions", insertions);
    }
}
