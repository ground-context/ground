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

package edu.berkeley.ground.postgres.dao.version;

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.factory.version.VersionSuccessorFactory;
import edu.berkeley.ground.common.model.version.Version;
import edu.berkeley.ground.common.model.version.VersionSuccessor;
import edu.berkeley.ground.common.utils.DbStatements;
import edu.berkeley.ground.common.utils.IdGenerator;
import edu.berkeley.ground.postgres.utils.PostgresStatements;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import play.db.Database;
import play.libs.Json;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class VersionSuccessorDao implements VersionSuccessorFactory {
  private final IdGenerator idGenerator;
  private final Database dbSource;

  //TODO: Should take in a Play Database connection instead of dbClient
  public VersionSuccessorDao(Database dbSource, IdGenerator idGenerator) {
    this.dbSource = dbSource;
    this.idGenerator = idGenerator;
  }

  public long getNewSuccessorId() {
    return this.idGenerator.generateSuccessorId();
  }

  //TODO: Create an addToSqlList method
  /**
   * Create a sqlList containing commands that will persist a new version successor
   * @param fromId id of the parent version
   * @param toId id of the child version
   * @return List of one sql expression
   * @throws GroundException an error creating the sql list
   */
  public PostgresStatements insert(long fromId, long toId, long versionSuccessorId) throws GroundException {

    PostgresStatements statements = new PostgresStatements();
    String sql = String.format("insert into version_successor (id, from_version_id, to_version_id) " +
        "values (%d,%d,%d)", versionSuccessorId, fromId, toId);
    statements.append(sql);
    return statements;
  }

  /**
   * Create and persist a version successor.
   *
   * @param fromId the id of the parent version
   * @param toId the id of the child version
   * @param <T> the types of the connected versions
   * @return the created version successor
   * @throws GroundException an error creating the successor
   */
  @Override
  public <T extends Version> VersionSuccessor<T> create(long fromId, long toId)
    throws GroundException {

    //TODO: Don't use dbClient
    long dbId = idGenerator.generateSuccessorId();
    return new VersionSuccessor<>(dbId, fromId, toId);
  }

  /**
   * Retrieve a version successor from the database.
   *
   * @param dbId the id of the successor to retrieve
   * @param <T> the types of the connected versions
   * @return the retrieved version successor
   * @throws GroundException either the successor didn't exist or couldn't be retrieved
   */
  @Override
  public <T extends Version> VersionSuccessor<T> retrieveFromDatabase(long dbId)
    throws GroundException {

    long fromId = 0L;
    long toId = 0L;

    try (Connection con = dbSource.getConnection()) {

      Statement stmt = con.createStatement();
      String sql = String.format("select * from version_successor where id=%d", dbId);
      final ResultSet resultSet = stmt.executeQuery(sql);
      if (resultSet.next()) {
        fromId = resultSet.getLong("from_version_id");
        toId = resultSet.getLong("to_version_id");
      }
    } catch (Exception e) {
      throw new GroundException(e);
    }

    return new VersionSuccessor<T>(dbId, fromId, toId);
  }

  /**
   * Delete a version successor from the database.
   *
   * @param toId the destination version
   */
  @Override
  public void deleteFromDestination(DbStatements statements, long toId, long itemId) throws GroundException {
    ResultSet resultSet;
    try {
      String sql = String.format("SELECT * FROM version_successor WHERE to_version_id = %d;", toId);
      JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
      System.out.println("");

      for (JsonNode result : json) {
        //System.out.println(result.get("version_successor_id").asLong());
        Long dbId = result.get("id").asLong();

        statements.append(String.format("DELETE FROM version_history_dag WHERE version_successor_id = %d;", dbId));
        statements.append(String.format("DELETE FROM version_successor WHERE id = %d;", dbId));
      }
      //PostgresUtils.executeSqlList(dbSource, (PostgresStatements) statements);
    } catch (Exception e) {
      throw new GroundException(e);
    }

  }
}
