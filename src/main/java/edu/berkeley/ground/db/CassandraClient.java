package edu.berkeley.ground.db;

import com.datastax.driver.core.*;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.exceptions.GroundDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class CassandraClient implements DBClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraClient.class);

    private Cluster cluster;

    private String keyspace;

    public CassandraClient(String host, int port, String dbName, String username, String password) {
        cluster = Cluster.builder()
                .addContactPoint(host + ":" + port)
                .withAuthProvider(new PlainTextAuthProvider(username, password))
                .build();

        this.keyspace = dbName;
    }

    public CassandraConnection getConnection() throws GroundDBException {
        try {
            return new CassandraConnection(cluster.connect(this.keyspace));
        } catch (SQLException e) {
            throw new GroundDBException(e);
        }
    }

    public class CassandraConnection extends GroundDBConnection {
        private Session session;

        public CassandraConnection(Session session) throws SQLException {
            this.session = session;
        }

        public void insert(String table, List<DbDataContainer> insertValues) throws GroundDBException {
            String insertString = "insert into " + table + "(";
            String valuesString = "values (";

            for (DbDataContainer container : insertValues) {
                insertString += container.getField() + ", ";
                valuesString += "?, ";
            }

            insertString = insertString.substring(0, insertString.length() - 2) + ")";
            valuesString = valuesString.substring(0, valuesString.length() - 2) + ")";

            String prepString = insertString + valuesString + ";";

            BoundStatement statement = new BoundStatement(session.prepare(prepString));

            int index = 1;
            for (DbDataContainer container : insertValues) {
                CassandraClient.setValue(statement, container.getValue(), container.getType(), index);

                index++;
            }

            LOGGER.info("Executing update: " + statement.toString() + ".");

            this.session.execute(statement);
        }

        public QueryResults equalitySelect(String table, List<String> projection, List<DbDataContainer> predicatesAndValues) throws GroundDBException {
            String select = "select ";
            for (String item : projection) {
                select += item + ", ";
            }

            select = select.substring(0, select.length() - 2) + " from " + table;

            if (predicatesAndValues.size() > 0) {
                select += " where ";

                for (DbDataContainer container : predicatesAndValues) {
                    select += container.getField() + " = ? and ";
                }

                select = select.substring(0, select.length() - 4);
            }

            BoundStatement statement = new BoundStatement(this.session.prepare(select + ";"));

            int index = 1;
            for (DbDataContainer container : predicatesAndValues) {
                CassandraClient.setValue(statement, container.getValue(), container.getType(), index);

                index++;
            }

            LOGGER.info("Executing query: " + statement.toString() + ".");

            return null;
        }

        public void commit() throws GroundDBException {
            // do nothing; Cassandra doesn't have txns
        }

        public void abort() throws GroundDBException {
            // do nothing; Cassandra doesn't have txns
        }
    }

    private static void setValue(BoundStatement statement, Object value, Type type, int index) {
        switch (type) {
            case STRING:
                if (value != null) {
                    statement.setString(index, (String) value);
                }

                break;
            case INTEGER:
                if (value != null) {
                    statement.setInt(index, (Integer) value);
                }
                break;
            case BOOLEAN:
                if (value != null) {
                    statement.setBool(index, (Boolean) value);
                }
                break;
        }
    }
}
