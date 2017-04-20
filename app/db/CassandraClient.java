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

package db;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
            .withPort(port)
            .withAuthProvider(new PlainTextAuthProvider(username, password))
            .build();

    this.session = this.cluster.connect(keyspace);
    this.preparedStatements = new HashMap<>();
  }

  /**
   * Returns the current session in this Cassandra Client
   * @return session the active Cassandra session used in this CassandraClient
   */
  public Session getSession() {
    return this.session;
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

    // Ensure the following tables have unique entries
    if (table.equals("edge") || table.equals("node") || table.equals("graph") || table.equals("structure")) {
      insert = insert.substring(0, insert.length() - 1) + "IF NOT EXISTS;";
    }
    BoundStatement statement = this.prepareStatement(insert);

    BoundStatement statement = bind(insert, insertValues);

    LOGGER.info("Executing update: " + statement.preparedStatement().getQueryString() + ".");
    this.session.execute(statement);
  }

  /**
   *
   * @param table the table to update
   * @param setName the name of the set (column) being appended to
   * @param values a set containing all values to be added. Set type must match with column type
   * @param predicates values specifying which row will be updated
	 */
  public void addToSet(String table, String setName,
                       Set<? extends Object> values, List<DbDataContainer> predicates) {
    this.modifySet(table, setName, values, predicates, true);
  }

  public void addToMap(String table, String mapName, Map<? extends Object, ? extends Object> keyValues,
                       List<DbDataContainer> predicates) {
    String predicatesString =
      predicates
        .stream()
        .map(predicate -> predicate.getField() + " = ?")
        .collect(Collectors.joining(" and "));
    String whereString = " where " + predicatesString + ";";
    String query = "UPDATE " + table + " SET " + mapName + " = " + mapName + " + " + " ? " + whereString;

    BoundStatement statement = this.prepareStatement(query);

    // Cannot use normal setValue method for collections: https://datastax-oss.atlassian.net/browse/JAVA-185
    statement.bind(keyValues);

    int index = 1;
    for (DbDataContainer container : predicates) {
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

    // This might not be very efficient https://www.datastax.com/dev/blog/allow-filtering-explained-2
    select += " ALLOW FILTERING;";

    BoundStatement statement = bind(select, predicatesAndValues);

    LOGGER.info("Executing query: " + statement.preparedStatement().getQueryString() + ".");
    ResultSet resultSet = this.session.execute(statement);

    return new CassandraResults(resultSet);
  }

  /**
   *
   * @param table the table to query
   * @param projection the set of columns to retrieve
   * @param collectionName the name of the set we will search
   * @param value the value searched for in the set
	 */
  public CassandraResults selectWhereCollectionContains(
    String table, List<String> projection, String collectionName, DbDataContainer value) {
    String query = "SELECT " + String.join(", ", projection) + " FROM " + table + " WHERE " + collectionName
      + " CONTAINS ?;";

    BoundStatement statement = this.prepareStatement(query);
    CassandraClient.setValue(statement, value.getValue(), value.getGroundType(), 0);

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

    BoundStatement statement = bind(updateString, setPredicates, wherePredicates);

    LOGGER.info("Executing update: " + statement.preparedStatement().getQueryString() + ".");
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

    BoundStatement statement = bind(deleteString, predicates);

    this.session.execute(statement);
  }

  private BoundStatement bind(String statement, List<DbDataContainer>... predicates) {
    BoundStatement boundStatement = this.prepareStatement(statement);
    List<Object> values = Arrays.stream(predicates).flatMap(Collection::stream)
      .map(p-> p.getValue()).collect(Collectors.toList());
    boundStatement.bind(values.toArray(new Object[values.size()]));
    return boundStatement;
  }

  /**
   * Deletes values from a set
   * @param table the table to update
   * @param setName the name of the set (column) being updated
   * @param values a set containing all values to be deleted. Set type must match with column type
   * @param predicates values specifying which row will be updated
   */
  public void deleteFromSet(String table, String setName,
                            Set<? extends Object> values, List<DbDataContainer> predicates) {
    this.modifySet(table, setName, values, predicates, false);
  }

  /**
   * Deletes a column from a table (sets column to null)
   * @param predicates the predicates used to match row(s) to delete from
   * @param table the table to delete from
   * @param columnName the column to delete
   */
  public void deleteColumn(List<DbDataContainer> predicates, String table, String columnName) {
    String deleteString = "delete " + columnName + " from " + table + " ";

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

  /**
   * Deletes all keys in the associated map by value
   * @param predicates the predicates to match by row
   * @param table the table to be deleted from
   * @param columnName the column name of the map
   * @param values the values in the map to be deleted
   */
  public void deleteFromMapByValue(List<DbDataContainer> predicates, String table,
                                   String columnName, List<DbDataContainer> values) {
    String deleteString = "delete " + columnName + "[?] from " + table;
    String predicateString = predicates.stream().map(predicate -> predicate.getField() + " = ? ")
      .collect(Collectors.joining(" and "));
    deleteString += " where " + predicateString;

    for (DbDataContainer val : values) {
      BoundStatement statement = this.prepareStatement(deleteString);

      // Set the value to be deleted
      CassandraClient.setValue(statement, val.getValue(), val.getGroundType(), 0);

      int index = 1;
      for (DbDataContainer predicate : predicates) {
        CassandraClient.setValue(statement, predicate.getValue(), predicate.getGroundType(), index);
        index++;
      }

      this.session.execute(statement);
    }
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

  /**
   * Adds or subtracts the specified elements from a set
   * @param table the table to update
   * @param setName the name of the set (column) being added to or subtracted from
   * @param values a set containing all relevant values. Set type must match with column type
   * @param predicates values specifying which row will be updated
   * @param add true if adding false if subtracting
   */
  private void modifySet(String table, String setName,
    Set<? extends Object> values, List<DbDataContainer> predicates, boolean add) {
    String symbol;
    if (add) {
      symbol = " + ";
    } else {
      symbol = " - ";
    }

    String predicatesString =
      predicates
        .stream()
        .map(predicate -> predicate.getField() + " = ?")
        .collect(Collectors.joining(" and "));
    String whereString = " where " + predicatesString + ";";
    String query = "UPDATE " + table + " SET " + setName + " = " + setName + symbol + " ? " + whereString;

    BoundStatement statement = this.prepareStatement(query);

    // Cannot use normal setValue method for collections: https://datastax-oss.atlassian.net/browse/JAVA-185
    statement.bind(values);

    int index = 1;
    for (DbDataContainer container : predicates) {
      CassandraClient.setValue(statement, container.getValue(), container.getGroundType(), index);

      index++;
    }

    LOGGER.info("Executing update: " + statement.preparedStatement().getQueryString() + ".");
    this.session.execute(statement);
  }

  private BoundStatement prepareStatement(String sql) {
    // Use the cached statement if possible; otherwise, prepare a new statement.
    PreparedStatement statement =
        this.preparedStatements.computeIfAbsent(sql, this.session::prepare);
    return new BoundStatement(statement);
  }

}
