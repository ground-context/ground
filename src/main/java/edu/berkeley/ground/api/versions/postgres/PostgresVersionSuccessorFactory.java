package edu.berkeley.ground.api.versions.postgres;

import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.api.versions.Version;
import edu.berkeley.ground.api.versions.VersionSuccessor;
import edu.berkeley.ground.api.versions.VersionSuccessorFactory;
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

public class PostgresVersionSuccessorFactory extends VersionSuccessorFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresVersionSuccessorFactory.class);

    public <T extends Version> VersionSuccessor<T> create(GroundDBConnection connection, String fromId, String toId) throws GroundException {
        List<DbDataContainer> insertions = new ArrayList<>();
        insertions.add(new DbDataContainer("vfrom", Type.STRING, fromId));
        insertions.add(new DbDataContainer("vto", Type.STRING, toId));

        connection.insert("VersionSuccessors", insertions);

        ResultSet resultSet = connection.equalitySelect("VersionSuccessors", Stream.of("successor_id").collect(Collectors.toList()), insertions);
        try {
            int dbId = resultSet.getInt(1);
            return VersionSuccessorFactory.construct(dbId, toId, fromId);
        } catch (SQLException e) {
            LOGGER.error("Unexpected error: " + e.getMessage());

            throw new GroundException(e);
        }

    }

    public <T extends Version> VersionSuccessor<T> retrieveFromDatabase(GroundDBConnection connection, int dbId) throws GroundException {
        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("successor_id", Type.INTEGER, dbId));

        ResultSet resultSet = connection.equalitySelect("VersionSuccessors", DBClient.SELECT_STAR, predicates);

        String toId = DbUtils.getString(resultSet, 2);
        String fromId = DbUtils.getString(resultSet, 3);

        return VersionSuccessorFactory.construct(dbId, toId, fromId);
    }
}
