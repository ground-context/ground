/*
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

import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundDBException;
import edu.berkeley.ground.exceptions.GroundException;

import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.types.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class Neo4jClient implements DBClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jClient.class);

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

    private String addValuesToStatement(String statement, List<DbDataContainer> values) {
      int count = 0;
      for (DbDataContainer container : values) {
        if (container.getValue() != null) {
          statement += container.getField() + " : ";

          switch (container.getGroundType()) {
            case STRING:
              statement += "'" + container.getValue().toString() + "'";
              break;
            case INTEGER:
              statement += (int) container.getValue();
              break;
            case BOOLEAN:
              statement += container.getValue();
              break;
            case LONG:
              statement += (long) container.getValue();
              break;
          }

          statement += ", ";
          count++;
        }
      }

      if (count > 0) {
        statement = statement.substring(0, statement.length() - 2);
      }

      return statement;
    }

    /**
     * Add a new vertex to the graph.
     *
     * @param label      the vertex label
     * @param attributes the vertex's attributes
     */
    public void addVertex(String label, List<DbDataContainer> attributes) {
      String insert = "CREATE (: " + label + " {";

      insert = this.addValuesToStatement(insert, attributes);

      insert += "})";

      this.transaction.run(insert);
    }

    /**
     * Add a new edge to the graph.
     *
     * @param label      the edge label
     * @param fromId     the id of the source vertex
     * @param toId       the id of the destination vertex
     * @param attributes the edge's attributes
     */
    public void addEdge(String label, long fromId, long toId, List<DbDataContainer> attributes) {
      String insert = "MATCH (f" + "{id : " + fromId + " })";
      insert += "MATCH (t" + "{id : " + toId + " })";
      insert += "CREATE (f)-[:" + label + "{";

      insert = this.addValuesToStatement(insert, attributes);

      insert += "}]->(t)";

      this.transaction.run(insert);
    }

    /**
     * Add a new edge to the graph.
     *
     * @param label      the edge label
     * @param fromId     the id of the source vertex
     * @param toId       the id of the destination vertex
     * @param attributes the edge's attributes
     */
    public void addEdge(String label, String fromId, long toId, List<DbDataContainer> attributes) {
      String insert = "MATCH (f" + "{id : '" + fromId + "' })";
      insert += "MATCH (t" + "{id : " + toId + " })";
      insert += "CREATE (f)-[:" + label + "{";

      insert = this.addValuesToStatement(insert, attributes);

      insert += "}]->(t)";

      this.transaction.run(insert);
    }

    /**
     * Add a new vertex and an edge connecting it to another vertex
     *
     * @param label          the vertex label
     * @param attributes     the vertex's attributes
     * @param edgeLabel      the edge label
     * @param fromId         the source of the edge
     * @param edgeAttributes the edge's attributes
     */
    public void addVertexAndEdge(String label, List<DbDataContainer> attributes, String edgeLabel, long fromId, List<DbDataContainer> edgeAttributes) {
      String insert = "MATCH (f {id: " + fromId + "})";
      insert += "CREATE (t: " + label + "{";

      insert = this.addValuesToStatement(insert, attributes);
      insert += "})";
      insert += "CREATE (f)-[e: " + edgeLabel + "{";

      insert = this.addValuesToStatement(insert, edgeAttributes);
      insert += "}]->(t)";

      this.transaction.run(insert);
    }

    /**
     * Retrieve a vertex.
     *
     * @param attributes the set of attributes to filter
     * @return the Record of the vertex
     */
    public Record getVertex(List<DbDataContainer> attributes) throws EmptyResultException {
      return this.getVertex(null, attributes);
    }

    /**
     * Get all vertices with a certain set of attributes.
     *
     * @param attributes the attributes to filter by
     */
    public List<Long> getVerticesByAttributes(List<DbDataContainer> attributes) {
      String query = "MATCH (f {";

      query = this.addValuesToStatement(query, attributes);
      query += "}) return f";

      StatementResult queryResult = this.session.run(query);

      List<Long> result = new ArrayList<>();
      while (queryResult.hasNext()) {
        result.add((Long) GroundType.stringToType(queryResult.next().get("f").asNode().get("id").toString(), GroundType.LONG));
      }

      return result;
    }

    /**
     * Get a vertex with a set of attributes and with a particular label.
     *
     * @param label      the vertex label
     * @param attributes the attributes to filter by
     * @return the Record with the vertex
     */
    public Record getVertex(String label, List<DbDataContainer> attributes) throws EmptyResultException {
      String query;
      if (label == null) {
        query = "MATCH (v {";
      } else {
        query = "MATCH (v:" + label + "{";
      }

      query = this.addValuesToStatement(query, attributes);
      query += "}) RETURN v";
      StatementResult result = this.transaction.run(query);

      if (result.hasNext()) {
        return result.next();
      }

      throw new EmptyResultException("No results found for query: " + query);
    }

    /**
     * Retrieve an edge.
     *
     * @param label      the edge label
     * @param attributes the attributes to filter by
     * @return the Neo4j Relationship for this edge
     */
    public Relationship getEdge(String label, List<DbDataContainer> attributes) throws EmptyResultException {
      String query = "MATCH (v)-[e:" + label + "{";

      query = this.addValuesToStatement(query, attributes);
      query += "}]->(w) RETURN e";

      StatementResult result = this.transaction.run(query);

      if (result.hasNext()) {
        Record r = result.next();

        return r.get("e").asRelationship();
      }

      throw new EmptyResultException("No results found for query: " + query);
    }

    /**
     * Get all the edges with a particular label that are reachable from a particular
     * starting vertex.
     *
     * @param startId the starting point for the query
     * @param label   the edge label we are looking for
     * @return the list of valid edges
     */
    public List<Relationship> getDescendantEdgesByLabel(long startId, String label) {

      String query = "MATCH (a)-[e:" + label + "]-(b " +
          "{node_id : " + startId + "}) WHERE a.id=" + startId +
          " OR a.node_id=" + startId + " RETURN DISTINCT e";

      StatementResult result = this.transaction.run(query);
      Set<Relationship> response = new HashSet<>();

      System.out.println(query);

      List<Record> resultList = result.list();
      if (!resultList.isEmpty()) {
        response.addAll(resultList.stream().map(r -> r.get("e").asRelationship()).collect(Collectors.toList()));
      }

      return new ArrayList<>(response);
    }

    /**
     * Get all vertices that are one edge away, where the edge connecting them has a
     * particular label.
     *
     * @param id           the vertex to start from
     * @param edgeLabel    the edge label we are looking for
     * @param returnFields the list of fields we want to select
     * @return a list of adjacent vertices related by edgeLabel
     */
    public List<Record> getAdjacentVerticesByEdgeLabel(String edgeLabel, long id, List<String> returnFields) {
      String query = "MATCH (a {id: " + id + " })";
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

    public List<Long> transitiveClosure(long nodeVersionId) throws GroundException {
      String query = "MATCH (a: NodeVersion {id: '" + nodeVersionId + "'})-[:EdgeVersionConnection*]->(b)";
      query += "RETURN b.id";

      List<Long> result = new ArrayList<>();
      List<Record> records = this.transaction.run(query).list();

      records.forEach(record -> result.add(record.get("b.id").asLong()));

      return result;
    }

    /**
     * For a particular object, set a given attribute.
     *
     * @param id       the id of the object
     * @param key      the key of the attribute
     * @param value    the value of the attribute
     * @param isString deteremines whether or not to encapsulate value in quotes
     */
    public void setProperty(long id, String key, Object value, boolean isString) {
      String insert = "MATCH (n {id: " + id + " })";

      if (isString) {
        insert += "set n." + key + " = '" + value.toString() + "'";
      } else {
        insert += "set n." + key + " = " + value.toString();
      }

      this.transaction.run(insert);
    }


    public List<Long> adjacentNodes(long nodeVersionId, String edgeNameRegex) throws GroundException {
      String query = "MATCH (n: NodeVersion {id: '" + nodeVersionId + "'})";
      query += "-[e: EdgeVersionConnection]->(evn: EdgeVersion) where evn.edge_id =~ '.*" + edgeNameRegex + ".*'";
      query += "MATCH (evn)-[f: EdgeVersionConnection]->(dst)";
      query += "return dst.id";

      List<Long> result = new ArrayList<>();
      List<Record> records = this.transaction.run(query).list();

      records.forEach(record -> {
        result.add(record.get("dst.id").asLong());
      });

      return result;
    }
  }

  public static String getStringFromValue(StringValue value) {
    String stringWithQuotes = value.toString();
    return stringWithQuotes.substring(1, stringWithQuotes.length() - 1);
  }
}
