package edu.berkeley.ground.api.versions.postgres;

import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.api.versions.VersionFactory;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.ArrayList;
import java.util.List;

public class PostgresVersionFactory extends VersionFactory {
    public void insertIntoDatabase(GroundDBConnection connection, String id) throws GroundException {
        List<DbDataContainer> insertions = new ArrayList<>();
        insertions.add(new DbDataContainer("id", Type.STRING, id));

        connection.insert("Versions", insertions);
    }
}