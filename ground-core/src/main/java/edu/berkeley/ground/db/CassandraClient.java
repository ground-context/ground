/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.ground.db;

import com.datastax.driver.core.*;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.util.JGraphTUtils;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class CassandraClient implements DBClient, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraClient.class);

  private final Cluster cluster;
  private final Session session;
  private final Map<String, PreparedStatement> preparedStatements;
  private final DirectedGraph<Long, DefaultEdge> graph;

  public CassandraClient(String host, int port, String keyspace, String username, String password) {
    this.cluster =
        Cluster.builder()
            .addContactPoint(host)
            .withAuthProvider(new PlainTextAuthProvider(username, password))
            .withPort(port)
            .build();

    this.session = this.cluster.connect(keyspace);
    this.preparedStatements = new HashMap<>();

    // at startup, load all nodes & edges into JGraphT for later in-memory processing
    ResultSet resultSet = this.session.execute("select id from node_version;");
    this.graph = JGraphTUtils.createGraph();

    for (Row r : resultSet.all()) {
      JGraphTUtils.addVertex(graph, r.getLong(0));
    }

    resultSet =
        this.session.execute("select from_node_version_id, to_node_version_id from edge_version;");

    for (Row r : resultSet.all()) {
      JGraphTUtils.addEdge(graph, r.getLong(0), r.getLong(1));
    }
  }

  /**
   * Insert a new row into table with insertValues.
   *
   * @param table the table to update
   * @param insertValues the values to put into table
   */
  public void insert(String table, List<DbDataContainer> insertValues) {
    // hack to keep JGraphT up to date
    if (table.equals("node_version")) {
      long id = -1;
      for (DbDataContainer container : insertValues) {
        if (container.getField().equals("id")) {
          id = (Long) container.getValue();
        }
      }

      JGraphTUtils.addVertex(this.graph, id);
    }
    if (table.equals("edge_version")) {
      long nvFromId = -1;
      long nvToId = -1;

      for (DbDataContainer container : insertValues) {
        if (container.getField().equals("from_node_version_id")) {
          nvFromId = (Long) container.getValue();
        }

        if (container.getField().equals("to_node_version_id")) {
          nvToId = (Long) container.getValue();
        }
      }

      JGraphTUtils.addEdge(this.graph, nvFromId, nvToId);
    }

    String fields =
        insertValues.stream().map(DbDataContainer::getField).collect(Collectors.joining(", "));
    String values = String.join(", ", Collections.nCopies(insertValues.size(), "?"));

    String insert = "insert into " + table + "(" + fields + ") values (" + values + ");";
    BoundStatement statement = this.prepareStatement(insert);

    int index = 0;
    for (DbDataContainer container : insertValues) {
      CassandraClient.setValue(statement, container.getValue(), container.getGroundType(), index);

      index++;
    }

    LOGGER.info("Executing update: " + statement.preparedStatement().getQueryString() + ".");
    this.session.execute(statement);
  }

  /**
   * Retrieve rows based on a set of predicates.
   *
   * @param table the table to query
   * @param projection the set of columns to retrieve
   * @param predicatesAndValues the predicates
   */
  public CassandraResults equalitySelect(
      String table, List<String> projection, List<DbDataContainer> predicatesAndValues)
      throws EmptyResultException {
    String items = String.join(", ", projection);
    String select = "select " + items + " from " + table;

    if (predicatesAndValues.size() > 0) {
      String predicatesString =
          predicatesAndValues
              .stream()
              .map(predicate -> predicate.getField() + " = ?")
              .collect(Collectors.joining(" and "));
      select += " where " + predicatesString;
    }

    select += " ALLOW FILTERING;";
    BoundStatement statement = this.prepareStatement(select);

    int index = 0;
    for (DbDataContainer container : predicatesAndValues) {
      CassandraClient.setValue(statement, container.getValue(), container.getGroundType(), index);

      index++;
    }

    LOGGER.info("Executing query: " + statement.preparedStatement().getQueryString() + ".");
    ResultSet resultSet = this.session.execute(statement);

    if (resultSet == null || resultSet.isExhausted()) {
      throw new EmptyResultException("No results found for query: " + statement.toString());
    }

    return new CassandraResults(resultSet);
  }

  @Override
  public List<Long> transitiveClosure(long nodeVersionId) {
    return JGraphTUtils.runDFS(this.graph, nodeVersionId);
  }

  public List<Long> adjacentNodes(long nodeVersionId, String edgeNameRegex) {
    BoundStatement statement =
        this.prepareStatement(
            "select to_node_version_id, edge_id from edge_version"
                + "where from_node_version_id = ? allow filtering;");

    statement.setLong(0, nodeVersionId);
    ResultSet resultSet = this.session.execute(statement);

    List<Long> result = new ArrayList<>();
    for (Row row : resultSet) {
      if (row.getString(1).contains(edgeNameRegex)) {
        result.add(row.getLong(0));
      }
    }

    return result;
  }

  @Override
  public void commit() {}

  @Override
  public void abort() {}

  @Override
  public void close() {
    this.session.close();
    this.cluster.close();
  }

  private BoundStatement prepareStatement(String sql) {
    // Use the cached statement if possible; otherwise, prepare a new statement.
    PreparedStatement statement =
        this.preparedStatements.computeIfAbsent(sql, this.session::prepare);
    return new BoundStatement(statement);
  }

  private static void setValue(
      BoundStatement statement, Object value, GroundType groundType, int index) {
    switch (groundType) {
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
      case LONG:
        if (value != null && (long) value != -1) {
          statement.setLong(index, (Long) value);
        } else {
          statement.setToNull(index);
        }
        break;
    }
  }
}
