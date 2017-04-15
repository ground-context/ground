/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.db;

import com.google.common.annotations.VisibleForTesting;

import edu.berkeley.ground.model.versions.GroundType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.types.Relationship;

public class Neo4jClient extends DbClient {
  private final Driver driver;
  private final Session session;
  private Transaction transaction;

  /**
   * Constructor for Neo4j client.
   *
   * @param host the Neo4j host
   * @param username the login username
   * @param password the login password
   */
  public Neo4jClient(String host, String username, String password) {
    this.driver = GraphDatabase.driver("bolt://" + host, AuthTokens.basic(username, password));
    this.session = this.driver.session();
    this.transaction = session.beginTransaction();
  }

  private String addValuesToStatement(String statement, List<DbDataContainer> values) {
    return statement + values.stream()
        .filter(container -> container.getValue() != null)
        .map(container -> {
          String stmt = container.getField() + " : ";
          if (container.getGroundType() == GroundType.STRING) {
            return stmt + "'" + container.getValue() + "'";
          } else {
            return stmt + container.getValue();
          }
        })
        .collect(Collectors.joining(", "));
  }

  /**
   * Add a new vertex to the graph.
   *
   * @param label the vertex label
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
   * @param label the edge label
   * @param fromId the id of the source vertex
   * @param toId the id of the destination vertex
   * @param attributes the edge's attributes
   */
  public void addEdge(String label, long fromId, long toId, List<DbDataContainer> attributes) {
    String insert =
        "MATCH (f"
            + "{id : "
            + fromId
            + " })"
            + "MATCH (t"
            + "{id : "
            + toId
            + " })"
            + "CREATE (f)-[:"
            + label
            + "{";

    insert = this.addValuesToStatement(insert, attributes);
    insert += "}]->(t)";

    this.transaction.run(insert);
  }

  /**
   * Add a new vertex and an edge connecting it to another vertex.
   *
   * @param label the vertex label
   * @param attributes the vertex's attributes
   * @param edgeLabel the edge label
   * @param fromId the source of the edge
   * @param edgeAttributes the edge's attributes
   */
  public void addVertexAndEdge(
      String label,
      List<DbDataContainer> attributes,
      String edgeLabel,
      long fromId,
      List<DbDataContainer> edgeAttributes) {

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
   * Get all vertices with a certain set of attributes.
   *
   * @param attributes the attributes to filter by
   */
  public List<Long> getVerticesByAttributes(List<DbDataContainer> attributes, String idAttribute) {
    String query = "MATCH (f {";

    query = this.addValuesToStatement(query, attributes);
    query += "}) where exists(f." + idAttribute + ") return f";

    StatementResult queryResult = this.transaction.run(query);

    List<Long> result = new ArrayList<>();
    while (queryResult.hasNext()) {
      result.add(
          (Long)
              GroundType.stringToType(
                  queryResult.next().get("f").asNode().get(idAttribute).toString(),
                  GroundType.LONG));
    }

    return result;
  }

  /**
   * Retrieve a vertex.
   *
   * @param attributes the set of attributes to filter
   * @return the Record of the vertex
   */
  public Record getVertex(List<DbDataContainer> attributes) {
    return this.getVertex(null, attributes);
  }

  /**
   * Get a vertex with a set of attributes and with a particular label.
   *
   * @param label the vertex label
   * @param attributes the attributes to filter by
   * @return the Record with the vertex
   */
  public Record getVertex(String label, List<DbDataContainer> attributes) {
    String query = "MATCH (v";

    if (label == null) {
      query += " {";
    } else {
      query += ":" + label + "{";
    }

    query = this.addValuesToStatement(query, attributes);
    query += "}) RETURN v";
    StatementResult result = this.transaction.run(query);

    if (result.hasNext()) {
      return result.next();
    } else {
      return null;
    }
  }

  /**
   * Retrieve an edge.
   *
   * @param label the edge label
   * @param attributes the attributes to filter by
   * @return the Neo4j Relationship for this edge
   */
  public Relationship getEdge(String label, List<DbDataContainer> attributes) {
    String query = "MATCH (v)-[e:" + label + "{";

    query = this.addValuesToStatement(query, attributes);
    query += "}]->(w) RETURN e";

    StatementResult result = this.transaction.run(query);

    if (result.hasNext()) {
      Record r = result.next();
      return r.get("e").asRelationship();
    } else {
      return null;
    }
  }

  /**
   * Get all the edges with a particular label that are reachable from a particular starting vertex.
   *
   * @param startId the starting point for the query
   * @param label the edge label we are looking for
   * @return the list of valid edges
   */
  public List<Relationship> getDescendantEdgesByLabel(long startId, String label) {
    String query = "MATCH (a {id: " + startId + "})-[e: " + label + "*]->(b) return distinct e;";
    StatementResult result = this.transaction.run(query);

    Set<Relationship> response = new HashSet<>();

    while (result.hasNext()) {
      List<Object> list = result.next().get("e").asList();
      list.forEach(s -> response.add((Relationship) s));
    }

    return new ArrayList<>(response);
  }

  /**
   * Get all vertices that are one edge away, where the edge connecting them has a particular label.
   *
   * @param id the vertex to start from
   * @param edgeLabel the edge label we are looking for
   * @param returnFields the list of fields we want to select
   * @return a list of adjacent vertices related by edgeLabel
   */
  public List<Record> getAdjacentVerticesByEdgeLabel(
      String edgeLabel, long id, List<String> returnFields) {
    String query =
        "MATCH (a {id: " + id + " })" + "MATCH (a)-[:" + edgeLabel + "]->(b)" + "RETURN ";

    query +=
        returnFields
            .stream()
            .map(field -> "b." + field + " as " + field)
            .collect(Collectors.joining(", "));

    StatementResult result = this.transaction.run(query);
    return result.list();
  }

  /**
   * For a particular object, set a given attribute.
   *
   * @param id the id of the object
   * @param key the key of the attribute
   * @param value the value of the attribute
   * @param isString determines whether or not to encapsulate value in quotes
   */
  public void setProperty(long id, String key, Object value, boolean isString) {
    String insert = "MATCH (n {id: " + id + " })" + "set n." + key + " = ";

    if (isString) {
      insert += "'" + value.toString() + "'";
    } else {
      insert += value.toString();
    }

    this.transaction.run(insert);
  }

  public void deleteNode(List<DbDataContainer> predicates, String label) {
    String delete = "MATCH (n: " + label + " {";

    delete = this.addValuesToStatement(delete, predicates);

    delete += "})" +
        "MATCH (n)-[e]-()" +
        "DELETE e " +
        "DELETE n;";

    this.transaction.run(delete);
  }

  @Override
  public void commit() {
    this.transaction.success();
    this.transaction.close();
    this.transaction = this.session.beginTransaction();
  }

  @Override
  public void abort() {
    this.transaction.failure();
    this.transaction.close();
    this.transaction = this.session.beginTransaction();
  }

  @Override
  public void close() {
    this.transaction.close();
    this.session.close();
    this.driver.close();
  }

  /**
   * Extract a Java String for a Neo4j StringValue.
   *
   * @param value the input StringValue
   * @return the extracted Java String
   */
  public static String getStringFromValue(StringValue value) {
    String stringWithQuotes = value.toString();
    return stringWithQuotes.substring(1, stringWithQuotes.length() - 1);
  }

  /**
   * Drop all the data in the Neo4j instance. Only should be used for test purposes.
   */
  @VisibleForTesting
  public void dropData() {
    Session session = this.driver.session();
    Transaction transaction = session.beginTransaction();
    transaction.run("MATCH ()-[e]->() DELETE e;");
    transaction.run("MATCH (n) DELETE n;");

    transaction.success();
    transaction.close();

    session.close();
  }
}
