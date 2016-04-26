package edu.berkeley.ground.api.versions;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class VersionSuccessor<T extends Version> {
    private static final Logger LOGGER = LoggerFactory.getLogger(VersionSuccessor.class);

    // the unique id of this VersionSuccessor
    private long id;

    // the id of the Version that originates this successor
    private String fromId;

    // the id of the Version that this success points to
    private String toId;

    protected VersionSuccessor(long id, String fromId, String toId) {
        this.id = id;
        this.fromId = fromId;
        this.toId = toId;
    }

    @JsonProperty
    public long getId() {
        return this.id;
    }

    @JsonProperty
    public String getFromId() {
        return this.fromId;
    }

    public String getToId() {
        return this.toId;
    }


    /* FACTORY METHODS */
    public static <T extends Version> VersionSuccessor<T> create(GroundDBConnection connection, String fromId, String toId) throws GroundException {
        List<DbDataContainer> insertions = new ArrayList<>();
        insertions.add(new DbDataContainer("vfrom", Type.STRING, fromId));
        insertions.add(new DbDataContainer("vto", Type.STRING, toId));

        connection.insert("VersionSuccessors", insertions);

        ResultSet resultSet = connection.equalitySelect("VersionSuccessors", Stream.of("successor_id").collect(Collectors.toList()), insertions);
        try {
            int dbId = resultSet.getInt(1);
            return new VersionSuccessor<T>(dbId, fromId, toId);
        } catch (SQLException e) {
            LOGGER.error("Unexpected error: " + e.getMessage());

            throw new GroundException(e);
        }

    }

    public static <T extends Version> VersionSuccessor<T> retrieveFromDatabase(GroundDBConnection connection, int dbId) throws GroundException {
        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("successor_id", Type.INTEGER, dbId));

        ResultSet resultSet = connection.equalitySelect("VersionSuccessors", DBClient.SELECT_STAR, predicates);

        String toId = DbUtils.getString(resultSet, 2);
        String fromId = DbUtils.getString(resultSet, 3);

        return new VersionSuccessor<>(dbId, toId, fromId);
    }
}
