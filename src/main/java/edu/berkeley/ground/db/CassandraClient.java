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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import edu.berkeley.ground.model.versions.GroundType;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraClient extends DbClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraClient.class);

  private final Cluster cluster;
  private final Session session;
  private final Map<String, PreparedStatement> preparedStatements;

  /**
   * Constructor for the Cassandra client.
   *
   * @param host the host address for Cassandra
   * @param port the Cassandra port
   * @param keyspace the name of the keyspace we're using
   * @param username the login username
   * @param password the login password
   */
  public CassandraClient(String host, int port, String keyspace, String username, String password) {
    this.cluster =
        Cluster.builder()
            .addContactPoint(host)
            .withAuthProvider(new PlainTextAuthProvider(username, password))
            .build();

    this.session = this.cluster.connect(keyspace);
    this.preparedStatements = new HashMap<>();
  }

  /**
   * Insert a new row into table with insertValues.
   *
   * @param table the table to update
   * @param insertValues the values to put into table
   */
  public void insert(String table, List<DbDataContainer> insertValues) {
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
  public CassandraResults equalitySelect(String table,
                                         List<String> projection,
                                         List<DbDataContainer> predicatesAndValues) {
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

    return new CassandraResults(resultSet);
  }

  /**
   * Execute an update statement in Cassandra.
   *
   * @param setPredicates the set portion of the update statement
   * @param wherePredicates the where portion of the update statement
   * @param table the table to update
   */
  public void update(List<DbDataContainer> setPredicates,
                     List<DbDataContainer> wherePredicates,
                     String table) {

    String updateString = "update " + table + " set ";

    if (setPredicates.size() > 0) {
      String setPredicateString = setPredicates.stream()
          .map(predicate -> predicate.getField() + " = ?")
          .collect(Collectors.joining(", "));

      updateString += setPredicateString;
    }

    if (wherePredicates.size() > 0) {
      String wherePredicateString = wherePredicates.stream()
          .map(predicate -> predicate.getField() + " = ?")
          .collect(Collectors.joining(" and "));

      updateString += " where " + wherePredicateString;
    }

    BoundStatement statement = this.prepareStatement(updateString);

    int index = 0;
    for (DbDataContainer predicate : setPredicates) {
      CassandraClient.setValue(statement, predicate.getValue(), predicate.getGroundType(), index);
      index++;
    }

    for (DbDataContainer predicate : wherePredicates) {
      CassandraClient.setValue(statement, predicate.getValue(), predicate.getGroundType(), index);
      index++;
    }

    this.session.execute(statement);
  }

  /**
   * Delete a row from a table.
   *
   * @param predicates the delete predicates
   * @param table the table to delete from
   */
  public void delete(List<DbDataContainer> predicates, String table) {
    String deleteString = "delete from " + table + " ";

    String predicateString = predicates.stream().map(predicate -> predicate.getField() + " = ? ")
        .collect(Collectors.joining(" and "));

    deleteString += "where " + predicateString;

    BoundStatement statement = this.prepareStatement(deleteString);

    int index = 0;
    for (DbDataContainer predicate : predicates) {
      CassandraClient.setValue(statement, predicate.getValue(), predicate.getGroundType(), index);
      index++;
    }

    this.session.execute(statement);
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

  private static void setValue(BoundStatement statement,
                               Object value,
                               GroundType groundType,
                               int index) {
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
      default:
        // impossible because we've listed all the enum types
        break;
    }
  }
}
