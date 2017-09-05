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

package edu.berkeley.ground.cassandra.dao.version;

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.common.dao.version.VersionSuccessorDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.exception.GroundException.ExceptionType;
import edu.berkeley.ground.common.model.version.VersionSuccessor;
import edu.berkeley.ground.common.util.DbStatements;
import edu.berkeley.ground.common.util.IdGenerator;
import edu.berkeley.ground.cassandra.dao.CqlConstants;
import edu.berkeley.ground.cassandra.util.CassandraDatabase;
import edu.berkeley.ground.cassandra.util.CassandraStatements;
import edu.berkeley.ground.cassandra.util.CassandraUtils;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import play.libs.Json;


public class CassandraVersionSuccessorDao {

  private final IdGenerator idGenerator;
  private final CassandraDatabase dbSource;

  public CassandraVersionSuccessorDao(CassandraDatabase dbSource, IdGenerator idGenerator) {
    this.dbSource = dbSource;
    this.idGenerator = idGenerator;
  }

  /**
   * Create a cqlList containing commands that will persist a new version successor
   *
   * @param successor the successor to insert
   * @return List of CQL expressions to insert version successor
   */
  public CassandraStatements insert(VersionSuccessor successor) throws GroundException {
    // Check to see if both are valid ids since we don't have foreign key constraints
    verifyVersion(successor.getFromId());
    verifyVersion(successor.getFromId());
       
    CassandraStatements statements = new CassandraStatements();
    String cql = String.format(CqlConstants.INSERT_VERSION_SUCCESSOR, successor.getId(), successor.getFromId(), successor.getToId());
    statements.append(cql);

    return statements;
  }

  /**
   * Create and persist a version successor.
   *
   * @param fromId the id of the parent version
   * @param toId the id of the child version
   * @return the created version successor
   */
  VersionSuccessor instantiateVersionSuccessor(long fromId, long toId) throws GroundException {
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
  public VersionSuccessor retrieveFromDatabase(long dbId) throws GroundException {
    try {
      String cql = String.format(CqlConstants.SELECT_VERSION_SUCCESSOR, dbId);
      JsonNode json = Json.parse(CassandraUtils.executeQueryToJson(dbSource, cql));

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
  public void deleteFromDestination(DbStatements statementsPointer, long toId, long itemId) throws GroundException {
    CassandraStatements statements = (CassandraStatements) statementsPointer;

    try {
      String cql = String.format(CqlConstants.SELECT_VERSION_SUCCESSOR_BY_ENDPOINT, toId);
      JsonNode json = Json.parse(CassandraUtils.executeQueryToJson(dbSource, cql));

      for (JsonNode result : json) {
        Long dbId = result.get("id").asLong();

        statements.append(String.format(CqlConstants.DELETE_SUCCESSOR_FROM_DAG, itemId, dbId));
        statements.append(String.format(CqlConstants.DELETE_VERSION_SUCCESSOR, dbId));
      }
    } catch (Exception e) {
      throw new GroundException(e);
    }
  }

  /**
   * Verify that id is a valid id since foreign key constraints don't exist.
   *
   * @param id an idea of a version
   */
  private void verifyVersion(long id) throws GroundException {
    if (id == 0L ) {
      return;
    }

    Session session = this.dbSource.getSession();

    String cql = String.format(CqlConstants.SELECT_STAR_BY_ID, "version", id);
    ResultSet resultSet = session.execute(cql);

    if (resultSet.isExhausted()) {
      throw new GroundException(ExceptionType.VERSION_NOT_FOUND, "version", String.valueOf(id));
    }
  }
}
