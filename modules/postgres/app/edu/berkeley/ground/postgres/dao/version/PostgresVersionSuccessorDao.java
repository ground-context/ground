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
import edu.berkeley.ground.common.exception.GroundException.ExceptionType;
import edu.berkeley.ground.common.model.version.VersionSuccessor;
import edu.berkeley.ground.common.util.DbStatements;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.postgres.dao.SqlConstants;
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
   * @param successor the successor to insert
   * @return List of SQL expressions to insert version successor
   */
  @Override
  public PostgresStatements insert(VersionSuccessor successor) {
    PostgresStatements statements = new PostgresStatements();
    String sql = String.format(SqlConstants.INSERT_VERSION_SUCCESSOR, successor.getId(), successor.getFromId(), successor.getToId());
    statements.append(sql);

    return statements;
  }

  /**
   * Create and persist a version successor.
   *
   * @param fromId the id of the parent version
   * @param toId the id of the child version
   * @return the created version successor
   */
  VersionSuccessor instantiateVersionSuccessor(long fromId, long toId) {
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
      String sql = String.format(SqlConstants.SELECT_VERSION_SUCCESSOR, dbId);
      JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));

      if (json.size() == 0) {
        throw new GroundException(ExceptionType.OTHER, String.format("Version Successor with id %d does not exist.", dbId));
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
      String sql = String.format(SqlConstants.SELECT_VERSION_SUCCESSOR_BY_ENDPOINT, toId);
      JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));

      for (JsonNode result : json) {
        Long dbId = result.get("id").asLong();

        statements.append(String.format(SqlConstants.DELETE_SUCCESSOR_FROM_DAG, dbId));
        statements.append(String.format(SqlConstants.DELETE_VERSION_SUCCESSOR, dbId));
      }
    } catch (Exception e) {
      throw new GroundException(e);
    }

  }
}
