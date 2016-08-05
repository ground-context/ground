package edu.berkeley.ground.db;

import edu.berkeley.ground.exceptions.GroundDBException;
import edu.berkeley.ground.exceptions.GroundException;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.v1.*;

import java.util.ArrayList;
import java.util.List;

public class Neo4jClient implements DBClient {
    private Driver driver;

    public Neo4jClient(String host, String username, String password) {
        this.driver = GraphDatabase.driver("bolt://" + host, AuthTokens.basic(username, password));
    }

    public Neo4jConnection getConnection() {
        return new Neo4jConnection(this.driver.session());
    }

    public class Neo4jConnection extends GroundDBConnection {
        private Transaction transaction;

        public Neo4jConnection(Session session) {
            this.transaction = session.beginTransaction();
        }

        public void addVertex(String label, List<DbDataContainer> attributes) {
            String insert = "CREATE (: " + label + " {";

            int count = 0;
            for (DbDataContainer container : attributes) {
                insert += container.getField() + " : ";

                switch (container.getGroundType()) {
                    case STRING: insert += "'" + container.getValue().toString() + "'"; break;
                    case INTEGER: insert += (int) container.getValue(); break;
                    case BOOLEAN: insert += container.getValue(); break;
                }

                if (++count < attributes.size()) {
                    insert += ", ";
                }
            }

            insert += "})";

            this.transaction.run(insert);
        }

        public void addEdge(String label, String fromId, String toId, List<DbDataContainer> attributes) {
            String insert = "MATCH (f" + "{id : '" + fromId + "' })";
            insert += "MATCH (t" + "{id : '" + toId + "' })";
            insert += "CREATE (f)-[:" + label + "{";

            int count = 0;
            for (DbDataContainer container : attributes) {
                insert += container.getField() + " : ";

                switch (container.getGroundType()) {
                    case STRING: insert += "'" + container.getValue().toString() + "'"; break;
                    case INTEGER: insert += (int) container.getValue(); break;
                    case BOOLEAN: insert += container.getValue(); break;
                }

                if (++count < attributes.size()) {
                    insert += ", ";
                }
            }

            insert += "}]->(t)";

            this.transaction.run(insert);
        }

        public void addVertexAndEdge(String label, List<DbDataContainer> attributes, String edgeLabel, String fromId, List<DbDataContainer> edgeAttributes) {
            String insert = "MATCH (f {id: '" + fromId  + "'})";
            insert += "CREATE (t: " + label + "{";

            int count = 0;
            for (DbDataContainer container : attributes) {
                insert += container.getField() + " : ";

                switch (container.getGroundType()) {
                    case STRING: insert += "'" + container.getValue().toString() + "'"; break;
                    case INTEGER: insert += (int) container.getValue(); break;
                    case BOOLEAN: insert += container.getValue(); break;
                }

                if (++count < attributes.size()) {
                    insert += ", ";
                }
            }

            insert += "})";

            insert += "CREATE (f)-[e: " + edgeLabel + "{";

            count = 0;
            for (DbDataContainer container : edgeAttributes) {
                insert += container.getField() + " : ";

                switch (container.getGroundType()) {
                    case STRING: insert += "'" + container.getValue().toString() + "'"; break;
                    case INTEGER: insert += (int) container.getValue(); break;
                    case BOOLEAN: insert += container.getValue(); break;
                }

                if (++count < attributes.size()) {
                    insert += ", ";
                }
            }

            insert += "}]->(t)";

            this.transaction.run(insert);
        }

        public Record getVertex(List<DbDataContainer> attributes) {
            return this.getVertex(null, attributes);
        }

        public Record getVertex(String label, List<DbDataContainer> attributes) {
            String query;
            if (label == null) {
                query = "MATCH (v {";
            }
            else {
                query = "MATCH (v:" + label + "{";
            }

            int count = 0;
            for (DbDataContainer container : attributes) {
                query += container.getField() + " : ";

                switch (container.getGroundType()) {
                    case STRING: query += "'" + container.getValue().toString() + "'"; break;
                    case INTEGER: query += (int) container.getValue(); break;
                    case BOOLEAN: query += container.getValue(); break;
                }

                if (++count < attributes.size()) {
                    query += ", ";
                }
            }

            query += "}) RETURN v";

            StatementResult result = this.transaction.run(query);

            if (result.hasNext()) {
                return result.next();
            }

            return null;
        }

        public Record getEdge(String label, List<DbDataContainer> attributes) {
            String query = "MATCH (v)-(e:" + label + "{";

            int count = 0;
            for (DbDataContainer container : attributes) {
                query += container.getField() + " : ";

                switch (container.getGroundType()) {
                    case STRING: query += "'" + container.getValue().toString() + "'"; break;
                    case INTEGER: query += (int) container.getValue(); break;
                    case BOOLEAN: query += container.getValue(); break;
                }

                if (++count < attributes.size()) {
                    query += ", ";
                }
            }

            query += "})->(w) RETURN e";

            StatementResult result = this.transaction.run(query);

            if (result.hasNext()) {
                return result.next();
            }

            return null;
        }

        public List<Record> getDescendantEdgesByLabel(String startId, String label) {
            String query = "MATCH (a {id: '" + startId + "' })";
            query += "MATCH (a)-[e:" + label + "*]->(b)";
            query += "RETURN e";

            StatementResult result = this.transaction.run(query);

            return result.list();
        }

        public List<Record> getAdjacentVerticesByEdgeLabel(String edgeLabel, String id, List<String> returnFields) {
            String query = "MATCH (a {id: '" + id + "' })";
            query += "MATCH (a)-[:" + edgeLabel + "]->(b)";
            query += "RETURN ";

            int count = 0;
            for (String field : returnFields) {
                query += "b." + field + " as " + field;

                if (++count < returnFields.size()) {
                    query += ", ";
                }
            }

            StatementResult result = this.transaction.run(query);


            return result.list();
        }

        public void commit() throws GroundDBException {
            this.transaction.success();
            this.transaction.close();
        }

        public void abort() throws GroundDBException {
            this.transaction.failure();
            this.transaction.close();
        }

        public List<String> transitiveClosure(String nodeVersionId) throws GroundException {
            String query = "MATCH (a {id: '" + nodeVersionId + "'})";
            query += "MATCH (a)-[:EdgeVersionConnection*]->(b)";
            query += "RETURN b.id";

            List<String> result = new ArrayList<>();
            List<Record> records = this.transaction.run(query).list();

            records.stream().forEach(record -> result.add(getStringFromValue((StringValue) record.get("b.id"))));

            return result;
        }

        public void setProperty(String id, String key, Object value, boolean isString) {
            String insert = "MATCH (n {id: '" + id  + "' })";

            if (isString) {
                insert += "set n." + key + " = '" + value.toString() + "'";
            } else {
                insert += "set n." + key + " = " + value.toString();
            }

            this.transaction.run(insert);
        }
    }

    public static String getStringFromValue(StringValue value) {
        String stringWithQuotes = value.toString();
        return stringWithQuotes.substring(1, stringWithQuotes.length() - 1);
    }
}
