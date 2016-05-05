package edu.berkeley.ground.db;

import edu.berkeley.ground.exceptions.GroundDBException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class CassandraClient implements DBClient {
    private static final String JDBCString = "jdbc:cassandra://%s:%d/%s";

    private String connectionString;
    private String username;
    private String password;

    public CassandraClient(String host, int port, String dbName, String username, String password) {
        connectionString = String.format(CassandraClient.JDBCString, host, port, dbName);
        this.username = username;
        this.password = password;
    }

    public CassandraConnection getConnection() throws GroundDBException {
        try {
            return new CassandraConnection(DriverManager.getConnection(connectionString, username, password));
        } catch (SQLException e) {
            throw new GroundDBException(e);
        }
    }

    public class CassandraConnection extends GroundDBConnection {
        public CassandraConnection(Connection connection) throws SQLException {
            this.connection = connection;
            this.connection.setAutoCommit(false);
        }

        public void insert(String table, List<DbDataContainer> insertValues) throws GroundDBException {
        }

        public ResultSet equalitySelect(String table, List<String> projection, List<DbDataContainer> predicatesAndValues) throws GroundDBException {
            return null;
        }
    }

}
