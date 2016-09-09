/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.db;

import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundDBException;
import edu.berkeley.ground.exceptions.GroundException;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.RelationshipValue;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Relationship;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        private Session session;

        public Neo4jConnection(Session session) {
            this.transaction = session.beginTransaction();
            this.session = session;
        }

        public void addVertex(String label, List<DbDataContainer> attributes) {
            String insert = "CREATE (: " + label + " {";

            int count = 0;
            for (DbDataContainer container : attributes) {
                if (container.getValue() != null) {
                    insert += container.getField() + " : ";

                    switch (container.getGroundType()) {
                        case STRING:
                            insert += "'" + container.getValue().toString() + "'";
                            break;
                        case INTEGER:
                            insert += (int) container.getValue();
                            break;
                        case BOOLEAN:
                            insert += container.getValue();
                            break;
                    }

                    insert += ", ";
                    count++;
                }
            }

            if (count > 0) {
                insert = insert.substring(0, insert.length() - 2);
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
                if (container.getValue() != null) {
                    insert += container.getField() + " : ";

                    switch (container.getGroundType()) {
                        case STRING:
                            insert += "'" + container.getValue().toString() + "'";
                            break;
                        case INTEGER:
                            insert += (int) container.getValue();
                            break;
                        case BOOLEAN:
                            insert += container.getValue();
                            break;
                    }

                    insert += ", ";
                    count++;
                }
            }

            if (count > 0) {
                insert = insert.substring(0, insert.length() - 2);
            }

            insert += "}]->(t)";

            this.transaction.run(insert);
        }

        public void addVertexAndEdge(String label, List<DbDataContainer> attributes, String edgeLabel, String fromId, List<DbDataContainer> edgeAttributes) {
            String insert = "MATCH (f {id: '" + fromId  + "'})";
            insert += "CREATE (t: " + label + "{";

            int count = 0;
            for (DbDataContainer container : attributes) {
                if (container.getValue() != null) {
                    insert += container.getField() + " : ";

                    switch (container.getGroundType()) {
                        case STRING:
                            insert += "'" + container.getValue().toString() + "'";
                            break;
                        case INTEGER:
                            insert += (int) container.getValue();
                            break;
                        case BOOLEAN:
                            insert += container.getValue();
                            break;
                    }

                    insert += ", ";
                    count++;
                }
            }

            if (count > 0) {
                insert = insert.substring(0, insert.length() - 2);
            }

            insert += "})";
            insert += "CREATE (f)-[e: " + edgeLabel + "{";

            count = 0;
            for (DbDataContainer container : edgeAttributes) {
                if (container.getValue() != null) {
                    insert += container.getField() + " : ";

                    switch (container.getGroundType()) {
                        case STRING:
                            insert += "'" + container.getValue().toString() + "'";
                            break;
                        case INTEGER:
                            insert += (int) container.getValue();
                            break;
                        case BOOLEAN:
                            insert += container.getValue();
                            break;
                    }

                    insert += ", ";
                    count++;
                }
            }

            if (count > 0) {
                insert = insert.substring(0, insert.length() - 2);
            }

            insert += "}]->(t)";

            this.transaction.run(insert);
        }

        public Record getVertex(List<DbDataContainer> attributes) throws EmptyResultException {
            return this.getVertex(null, attributes);
        }

        public Record getVertex(String label, List<DbDataContainer> attributes) throws EmptyResultException {
            String query;
            if (label == null) {
                query = "MATCH (v {";
            }
            else {
                query = "MATCH (v:" + label + "{";
            }

            int count = 0;
            for (DbDataContainer container : attributes) {
                if (container.getValue() != null) {
                    query += container.getField() + " : ";

                    switch (container.getGroundType()) {
                        case STRING:
                            query += "'" + container.getValue().toString() + "'";
                            break;
                        case INTEGER:
                            query += (int) container.getValue();
                            break;
                        case BOOLEAN:
                            query += container.getValue();
                            break;
                    }

                    count ++;
                    query += ", ";
                }
            }

            if (count > 0) {
                query = query.substring(0, query.length() - 2);
            }

            query += "}) RETURN v";

            StatementResult result = this.transaction.run(query);

            if (result.hasNext()) {
                return result.next();
            }

            throw new EmptyResultException("No results found for query: " + query);
        }

        public Relationship getEdge(String label, List<DbDataContainer> attributes) throws EmptyResultException {
            String query = "MATCH (v)-[e:" + label + "{";

            int count = 0;
            for (DbDataContainer container : attributes) {
                if (container.getValue() != null) {
                    query += container.getField() + " : ";

                    switch (container.getGroundType()) {
                        case STRING:
                            query += "'" + container.getValue().toString() + "'";
                            break;
                        case INTEGER:
                            query += (int) container.getValue();
                            break;
                        case BOOLEAN:
                            query += container.getValue();
                            break;
                    }

                    count++;
                    query += ", ";
                }
            }

            if (count > 0) {
                query = query.substring(0, query.length() - 2);
            }
            query += "}]->(w) RETURN e";

            StatementResult result = this.transaction.run(query);

            if (result.hasNext()) {
                Record r = result.next();

                return r.get("e").asRelationship();
            }

            throw new EmptyResultException("No results found for query: " + query);
        }

        public List<Relationship> getDescendantEdgesByLabel(String startId, String label) {
            String query = "MATCH (a {id: '" + startId + "' })";
            query += "MATCH (a)-[e:" + label + "*]->(b)";
            query += "RETURN e";

            StatementResult result = this.transaction.run(query);

            Set<Relationship> response = new HashSet<>();

            List<Record> resultList = result.list();

            if (!resultList.isEmpty()) {
                for (Record r : resultList) {
                    ListValue lv = (ListValue) r.get("e");

                    for (int i = 0; i < lv.size(); i++) {
                        response.add(lv.get(i).asRelationship());
                    }
                }
            }

            return new ArrayList<>(response);
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
            this.session.close();
        }

        public void abort() throws GroundDBException {
            this.transaction.failure();
            this.transaction.close();
            this.session.close();
        }

        public List<String> transitiveClosure(String nodeVersionId) throws GroundException {
            String query = "MATCH (a: NodeVersion {id: '" + nodeVersionId + "'})-[:EdgeVersionConnection*]->(b)";
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


        public List<String> adjacentNodes(String nodeVersionId, String edgeNameRegex) throws GroundException {
            String query = "MATCH (n: NodeVersion {id: '" + nodeVersionId + "'})";
            query += "-[e: EdgeVersionConnection]->(evn: EdgeVersion) where evn.edge_id =~ '.*" + edgeNameRegex + ".*'";
            query += "MATCH (evn)-[f: EdgeVersionConnection]->(dst)";
            query += "return dst.id";

            List<String> result = new ArrayList<>();
            List<Record> records = this.transaction.run(query).list();

            records.stream().forEach(record -> {
                result.add(record.get("dst.id").asString());
            });

            return result;
        }
    }

    public static String getStringFromValue(StringValue value) {
        String stringWithQuotes = value.toString();
        return stringWithQuotes.substring(1, stringWithQuotes.length() - 1);
    }
}
