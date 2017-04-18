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

import edu.berkeley.ground.exceptions.GroundDbException;
import edu.berkeley.ground.model.versions.GroundType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.postgresql.PGStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresClient extends DbClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresClient.class);
  private static final String JDBCString = "jdbc:postgresql://%s:%d/%s?stringtype=unspecified";

  private final Connection connection;
  private final Map<String, PreparedStatement> preparedStatements;

  /**
   * Constructor for Postgres client.
   *
   * @param host the host of the Postgres instance
   * @param port the Postgres port
   * @param dbName the name of the database in PG
   * @param username the login username
   * @param password the login password
   * @throws GroundDbException error in opening a connection to Postgres
   */
  public PostgresClient(String host, int port, String dbName, String username, String password)
      throws GroundDbException {
    String url = String.format(PostgresClient.JDBCString, host, port, dbName);
    try {
      this.connection = DriverManager.getConnection(url, username, password);
      this.connection.setAutoCommit(false);
    } catch (SQLException e) {
      throw new GroundDbException(e);
    }

    this.preparedStatements = new HashMap<>();
  }

  /**
   * Insert a new row into table with insertValues.
   *
   * @param table the table to update
   * @param insertValues the values to put into table
   */
  public void insert(String table, List<DbDataContainer> insertValues) throws GroundDbException {
    String fields =
        insertValues.stream().map(DbDataContainer::getField).collect(Collectors.joining(", "));
    String values = String.join(", ", Collections.nCopies(insertValues.size(), "?"));

    String insert = "insert into " + table + "(" + fields + ") values (" + values + ");";
    try {
      PreparedStatement preparedStatement = this.prepareStatement(insert);
      int index = 1;
      for (DbDataContainer container : insertValues) {
        PostgresClient.setValue(
            preparedStatement, container.getValue(), container.getGroundType(), index);

        index++;
      }

      LOGGER.info("Executing update: " + preparedStatement.toString() + ".");

      preparedStatement.executeUpdate();
    } catch (SQLException e) {
      LOGGER.error("Unexpected error in database insertion: " + e.getMessage());

      throw new GroundDbException(e);
    }
  }

  /**
   * Retrieve rows based on a set of predicates.
   *
   * @param table the table to query
   * @param projection the set of columns to retrieve
   * @param predicatesAndValues the predicates
   */
  public PostgresResults equalitySelect(
      String table, List<String> projection, List<DbDataContainer> predicatesAndValues)
      throws GroundDbException {
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

    select += ";";
    try {
      PreparedStatement preparedStatement = this.prepareStatement(select);
      int index = 1;
      for (DbDataContainer container : predicatesAndValues) {
        PostgresClient.setValue(
            preparedStatement, container.getValue(), container.getGroundType(), index);

        index++;
      }

      LOGGER.info("Executing query: " + preparedStatement.toString() + ".");

      ResultSet resultSet = preparedStatement.executeQuery();

      // Moves the cursor to the first element so that data can be accessed directly.
      return new PostgresResults(resultSet);
    } catch (SQLException e) {
      LOGGER.error("Unexpected error in database query: " + e.getMessage());

      throw new GroundDbException(e);
    }
  }

  /**
   * Execute an update statement in Cassandra.
   *
   * @param setPredicates the set portion of the update statement
   * @param wherePredicates the where portion of the update statement
   * @param table the table to update
   * @throws GroundDbException an error while executing the update
   */
  public void update(List<DbDataContainer> setPredicates, List<DbDataContainer> wherePredicates,
                     String table) throws GroundDbException {

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

    PreparedStatement statement = this.prepareStatement(updateString);

    try {
      int index = 1;
      for (DbDataContainer predicate : setPredicates) {
        PostgresClient.setValue(statement, predicate.getValue(), predicate.getGroundType(), index);
        index++;
      }


      for (DbDataContainer predicate : wherePredicates) {
        PostgresClient.setValue(statement, predicate.getValue(), predicate.getGroundType(), index);
        index++;
      }

      statement.executeUpdate();
    } catch (SQLException e) {
      throw new GroundDbException(e);
    }
  }

  /**
   * Delete a row from a table.
   *
   * @param predicates the delete predicates
   * @param table the table to delete from
   */
  public void delete(List<DbDataContainer> predicates, String table) throws GroundDbException {
    String deleteString = "delete from " + table + " ";

    String predicateString = predicates.stream().map(predicate -> predicate.getField() + " = ? ")
        .collect(Collectors.joining(", "));

    deleteString += "where " + predicateString;

    int index = 1;

    try {
      PreparedStatement statement = this.prepareStatement(deleteString);

      for (DbDataContainer predicate : predicates) {
        PostgresClient.setValue(statement, predicate.getValue(), predicate.getGroundType(), index);
        index++;
      }

      statement.executeUpdate();
    } catch (SQLException e) {
      throw new GroundDbException(e);
    }
  }

  @Override
  public void commit() throws GroundDbException {
    try {
      this.connection.commit();
    } catch (SQLException e) {
      throw new GroundDbException(e);
    }
  }

  @Override
  public void abort() throws GroundDbException {
    try {
      this.connection.rollback();
    } catch (SQLException e) {
      throw new GroundDbException(e);
    }
  }

  @Override
  public void close() throws GroundDbException {
    try {
      for (PreparedStatement statement : this.preparedStatements.values()) {
        statement.close();
      }

      this.connection.close();
    } catch (SQLException e) {
      throw new GroundDbException(e);
    }
  }

  private PreparedStatement prepareStatement(String sql) throws GroundDbException {
    // We cannot use computeIfAbsent, as prepareStatement throws an exception.
    // Check if the statement is already in the cache; if so, use it.
    PreparedStatement existingStatement = this.preparedStatements.get(sql);
    if (existingStatement != null) {
      return existingStatement;
    }

    try {
      // Otherwise, prepare the statement, then cache it.
      PreparedStatement newStatement = this.connection.prepareStatement(sql, ResultSet
          .TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

      this.preparedStatements.put(sql, newStatement);
      ((PGStatement) newStatement).setPrepareThreshold(10);
      return newStatement;
    } catch (SQLException e) {
      throw new GroundDbException(e);
    }
  }

  private static void setValue(
      PreparedStatement preparedStatement, Object value, GroundType groundType, int index)
      throws SQLException {
    switch (groundType) {
      case STRING:
        if (value == null) {
          preparedStatement.setNull(index, Types.VARCHAR);
        } else {
          preparedStatement.setString(index, (String) value);
        }
        break;
      case INTEGER:
        if (value == null) {
          preparedStatement.setNull(index, Types.INTEGER);
        } else {
          preparedStatement.setInt(index, (Integer) value);
        }
        break;
      case BOOLEAN:
        if (value == null) {
          preparedStatement.setNull(index, Types.BOOLEAN);
        } else {
          preparedStatement.setBoolean(index, (Boolean) value);
        }
        break;
      case LONG:
        if (value == null || (long) value == -1) {
          preparedStatement.setNull(index, Types.BIGINT);
        } else {
          preparedStatement.setLong(index, (Long) value);
        }
        break;
      default:
        // impossible because we've listed all enum values
        break;
    }
  }
}
