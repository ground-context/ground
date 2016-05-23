package edu.berkeley.ground.api.versions.cassandra;

import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.api.versions.Version;
import edu.berkeley.ground.api.versions.VersionSuccessor;
import edu.berkeley.ground.api.versions.VersionSuccessorFactory;
import edu.berkeley.ground.db.CassandraClient.CassandraConnection;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.QueryResults;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;

import java.util.ArrayList;
import java.util.List;

public class CassandraVersionSuccessorFactory extends VersionSuccessorFactory {
    public <T extends Version> VersionSuccessor<T> create(GroundDBConnection connectionPointer, String fromId, String toId) throws GroundException {
        CassandraConnection connection = (CassandraConnection) connectionPointer;

        List<DbDataContainer> insertions = new ArrayList<>();

        String dbId = IdGenerator.generateId(fromId + toId);

        insertions.add(new DbDataContainer("successor_id", Type.STRING, dbId));
        insertions.add(new DbDataContainer("vfrom", Type.STRING, fromId));
        insertions.add(new DbDataContainer("vto", Type.STRING, toId));

        connection.insert("VersionSuccessors", insertions);

        return VersionSuccessorFactory.construct(dbId, toId, fromId);
    }

    public <T extends Version> VersionSuccessor<T> retrieveFromDatabase(GroundDBConnection connectionPointer, String dbId) throws GroundException {
        CassandraConnection connection = (CassandraConnection) connectionPointer;

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("successor_id", Type.STRING, dbId));

        QueryResults resultSet = connection.equalitySelect("VersionSuccessors", DBClient.SELECT_STAR, predicates);

        String toId = resultSet.getString(2);
        String fromId = resultSet.getString(3);

        return VersionSuccessorFactory.construct(dbId, toId, fromId);
    }
}
