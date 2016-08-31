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

import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundDBException;
import edu.berkeley.ground.exceptions.GroundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PostgresClient implements DBClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresClient.class);

    private static final String JDBCString = "jdbc:postgresql://%s:%d/%s?stringtype=unspecified";
    private String connectionString;
    private String username;
    private String password;

    public PostgresClient(String host, int port, String dbName, String username, String password) {
        connectionString = String.format(PostgresClient.JDBCString, host, port, dbName);
        this.username = username;
        this.password = password;
    }

    public PostgresConnection getConnection() throws GroundDBException {
        try {
            return new PostgresConnection(DriverManager.getConnection(connectionString, username, password));
        } catch(SQLException e) {
            throw new GroundDBException(e);
        }
    }

    public class PostgresConnection extends GroundDBConnection {
        private Connection connection;

        public PostgresConnection(Connection connection) throws SQLException {
            this.connection = connection;
            this.connection.setAutoCommit(false);
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

            try {
                PreparedStatement preparedStatement = this.connection.prepareStatement(insertString + valuesString + ";");

                int index = 1;
                for (DbDataContainer container : insertValues) {
                    PostgresClient.setValue(preparedStatement, container.getValue(), container.getGroundType(), index);

                    index++;
                }

                LOGGER.info("Executing update: " + preparedStatement.toString() + ".");

                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                LOGGER.error("Unexpected error in database insertion: " + e.getMessage());

                throw new GroundDBException(e.getClass().toString() + ": " + e.getMessage());
            }

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

            try {
                PreparedStatement preparedStatement = this.connection.prepareStatement(select + ";");

                int index = 1;
                for (DbDataContainer container : predicatesAndValues) {
                    PostgresClient.setValue(preparedStatement, container.getValue(), container.getGroundType(), index);

                    index++;
                }

                LOGGER.info("Executing query: " + preparedStatement.toString() + ".");

                ResultSet resultSet = preparedStatement.executeQuery();
                if (!resultSet.isBeforeFirst()) {
                    throw new GroundDBException("No results found for query: " + preparedStatement.toString());
                }

                // Moves the cursor to the first element so that data can be accessed directly.
                resultSet.next();
                return new PostgresResults(resultSet);
            } catch (SQLException e) {
                LOGGER.error("Unexpected error in database query: " + e.getMessage());

                throw new GroundDBException(e.getMessage());
            }
        }

        public List<String> transitiveClosure(String nodeVersionId) throws GroundException {
            try {
                /*
                PreparedStatement statement = this.connection.prepareStatement("with recursive paths(vfrom, vto) as (\n" +
                                                    "    (select endpoint_one, endpoint_two from edgeversions where endpoint_one = ?)\n" +
                                                    "    union\n" +
                                                    "    (select p.vfrom, ev.endpoint_two\n" +
                                                    "    from paths p, edgeversions ev\n" +
                                                    "    where p.vto = ev.endpoint_one)\n" +
                                                    ") select vto from paths;"); */

                PreparedStatement statement = this.connection.prepareStatement("select reachable(?);");
                statement.setString(1, nodeVersionId);

                ResultSet resultSet = statement.executeQuery();

                List<String> result = new ArrayList<>();
                while (resultSet.next()) {
                    result.add(resultSet.getString(1));
                }

                return result;
            } catch (SQLException e) {
                throw new GroundException(e);
            }
        }

        public List<String> adjacentNodes(String nodeVersionId, String edgeNameRegex) throws GroundException {
            String query = "select endpoint_two from EdgeVersions ev where ev.endpoint_one = ?";
            query += " and ev.edge_id like ?;";

            edgeNameRegex = '%' + edgeNameRegex + '%';

            try {
                PreparedStatement statement = this.connection.prepareStatement(query);
                statement.setString(1, nodeVersionId);
                statement.setString(2, edgeNameRegex);

                ResultSet resultSet = statement.executeQuery();
                List<String> result = new ArrayList<>();

                while (resultSet.next()) {
                    result.add(resultSet.getString(1));
                }

                return result;
            } catch (SQLException e) {
                throw new GroundException(e);
            }
        }

        public void commit() throws GroundDBException {
            try {
                this.connection.commit();
                this.connection.close();
            } catch(SQLException e) {
                throw new GroundDBException(e);
            }
        }

        public void abort() throws GroundDBException {
            try {
                this.connection.rollback();
                this.connection.close();
            } catch (SQLException e) {
                throw new GroundDBException(e);
            }
        }
    }

    private static void setValue(PreparedStatement preparedStatement, Object value, GroundType groundType, int index) throws SQLException {
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
        }
    }
}
