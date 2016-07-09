package edu.berkeley.ground.db;

import com.datastax.driver.core.*;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.exceptions.GroundDBException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.JGraphTUtils;
import org.jgrapht.*;
import org.jgrapht.graph.DefaultEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class CassandraClient implements DBClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraClient.class);

    private Cluster cluster;

    private String keyspace;

    private DirectedGraph<String, DefaultEdge> graph;

    public CassandraClient(String host, int port, String dbName, String username, String password) throws GroundDBException {
        cluster = Cluster.builder()
                .addContactPoint(host)
                .withAuthProvider(new PlainTextAuthProvider(username, password))
                .build();

        this.keyspace = dbName;

        ResultSet resultSet = this.cluster.connect(this.keyspace).execute("select endpoint_one, endpoint_two from edgeversions;");
        this.graph = JGraphTUtils.createGraph();

        for (Row r : resultSet.all()) {
            JGraphTUtils.addEdge(graph, r.getString(0), r.getString(1));
        }
    }

    public CassandraConnection getConnection() throws GroundDBException {
        return new CassandraConnection(this.cluster.connect(this.keyspace), this.graph);
    }

    public class CassandraConnection extends GroundDBConnection {
        private Session session;
        private DirectedGraph<String, DefaultEdge> graph;

        public CassandraConnection(Session session, DirectedGraph<String, DefaultEdge> graph) {
            this.session = session;
            this.graph = graph;
        }

        public void insert(String table, List<DbDataContainer> insertValues) {
            // hack to keep JGraphT up to date
            if (table.equals("EdgeVersions")) {
                String nvFromId = null;
                String nvToId = null;

                for (DbDataContainer container : insertValues) {
                    if (container.getField().equals("endpoint_one")) {
                        nvFromId = container.getValue().toString();
                    }

                    if (container.getField().equals("endpoint_two")) {
                        nvToId = container.getValue().toString();
                    }
                }

                JGraphTUtils.addEdge(graph, nvFromId, nvToId);
            }

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

            int index = 0;
            for (DbDataContainer container : insertValues) {
                CassandraClient.setValue(statement, container.getValue(), container.getType(), index);

                index++;
            }

            LOGGER.info("Executing update: " + statement.preparedStatement().getQueryString() + ".");

            if(insertValues.size() > 1) {
                LOGGER.info("Size of insert values is: " + insertValues.size() + "; insert values[1] is " + insertValues.get(1).getValue() + "Value at first index is " + statement.getString(1));
            }

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

            int index = 0;
            for (DbDataContainer container : predicatesAndValues) {
                CassandraClient.setValue(statement, container.getValue(), container.getType(), index);

                index++;
            }

            LOGGER.info("Executing query: " + statement.preparedStatement().getQueryString() + ".");


            ResultSet resultSet = this.session.execute(statement);

            if(resultSet == null || resultSet.isExhausted()) {
                throw new GroundDBException("No results found for query: " + statement.toString());
            }

            return new CassandraResults(resultSet);
        }

        public List<String> transitiveClosure(String nodeVersionId) throws GroundException {

            return JGraphTUtils.iterate(this.graph, nodeVersionId);
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
                } else {
                    statement.setToNull(index);
                }

                break;
            case INTEGER:
                if (value != null) {
                    statement.setInt(index, (Integer) value);
                } else {
                    statement.setToNull(index);
                }
                break;
            case BOOLEAN:
                if (value != null) {
                    statement.setBool(index, (Boolean) value);
                } else {
                    statement.setToNull(index);
                }
                break;
        }
    }
}
