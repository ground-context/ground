/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.postgres.dao.version;

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.common.dao.version.VersionSuccessorDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.version.VersionSuccessor;
import edu.berkeley.ground.common.util.DbStatements;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.util.PostgresStatements;
import edu.berkeley.ground.postgres.util.PostgresUtils;
import play.db.Database;
import play.libs.Json;

public class PostgresVersionSuccessorDao implements VersionSuccessorDao {

  private final IdGenerator idGenerator;
  private final Database dbSource;

  public PostgresVersionSuccessorDao(Database dbSource, IdGenerator idGenerator) {
    this.dbSource = dbSource;
    this.idGenerator = idGenerator;
  }

  /**
   * Create a sqlList containing commands that will persist a new version successor
   *
   * @param fromId id of the parent version
   * @param toId id of the child version
   * @return List of one sql expression
   * @throws GroundException an error creating the sql list
   */
  public PostgresStatements insert(long fromId, long toId, long versionSuccessorId)
    throws GroundException {

    PostgresStatements statements = new PostgresStatements();
    String sql = String.format("insert into version_successor (id, from_version_id, to_version_id) " +
                                 "values (%d,%d,%d)", versionSuccessorId, fromId, toId);
    statements.append(sql);
    return statements;
  }

  @Override
  public VersionSuccessor create(long fromId, long toId)
    throws GroundException {
    long dbId = idGenerator.generateSuccessorId();
    PostgresStatements statements = insert(fromId, toId, dbId);
    PostgresUtils.executeSqlList(dbSource, statements);
    return new VersionSuccessor(dbId, fromId, toId);
  }

  /**
   * Create and persist a version successor.
   *
   * @param fromId the id of the parent version
   * @param toId the id of the child version
   * @return the created version successor
   * @throws GroundException an error creating the successor
   */
  protected VersionSuccessor instantiateVersionSuccessor(long fromId, long toId)
    throws GroundException {

    long dbId = idGenerator.generateSuccessorId();
    return new VersionSuccessor(dbId, fromId, toId);
  }

  /**
   * Retrieve a version successor from the database.
   *
   * @param dbId the id of the successor to retrieve
   * @return the retrieved version successor
   * @throws GroundException either the successor didn't exist or couldn't be retrieved
   */
  @Override
  public VersionSuccessor retrieveFromDatabase(long dbId) throws GroundException {
    try {
      String sql = String.format("select * from version_successor where id=%d", dbId);
      JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
      if (json.size() == 0) {
        throw new GroundException(String.format("Version Successor with id %d does not exist.", dbId));
      }

      json = json.get(0);
      return new VersionSuccessor(dbId, json.get("fromVersionId").asLong(), json.get("toVersionId").asLong());
    } catch (Exception e) {
      throw new GroundException(e);
    }

  }

  /**
   * Delete a version successor from the database.
   *
   * @param statementsPointer the list of DB statements to append to
   * @param toId the destination version
   * @param itemId the id of the item we are deleting from
   * @throws GroundException an unexpected error while retrieving the version successors to delete
   */
  @Override
  public void deleteFromDestination(DbStatements statementsPointer, long toId, long itemId) throws GroundException {
    PostgresStatements statements = (PostgresStatements) statementsPointer;

    try {
      String sql = String.format("SELECT * FROM version_successor WHERE to_version_id = %d;", toId);
      JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));

      for (JsonNode result : json) {
        Long dbId = result.get("id").asLong();

        statements.append(String.format("DELETE FROM version_history_dag WHERE version_successor_id = %d;", dbId));
        statements.append(String.format("DELETE FROM version_successor WHERE id = %d;", dbId));
      }
    } catch (Exception e) {
      throw new GroundException(e);
    }

  }
}
