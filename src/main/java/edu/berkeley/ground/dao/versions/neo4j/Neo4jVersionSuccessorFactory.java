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

package edu.berkeley.ground.dao.versions.neo4j;

import edu.berkeley.ground.dao.versions.VersionSuccessorFactory;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundDbException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.model.versions.Version;
import edu.berkeley.ground.model.versions.VersionSuccessor;
import edu.berkeley.ground.util.IdGenerator;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.types.Relationship;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class Neo4jVersionSuccessorFactory extends VersionSuccessorFactory {
  private final Neo4jClient dbClient;
  private final IdGenerator idGenerator;

  public Neo4jVersionSuccessorFactory(Neo4jClient dbClient, IdGenerator idGenerator) {
    this.dbClient = dbClient;
    this.idGenerator = idGenerator;
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

    // check if both IDs exist
    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("id", GroundType.LONG, fromId));
    Record record;

    try {
      record = this.dbClient.getVertex(predicates);
    } catch (EmptyResultException e) {
      throw new GroundDbException("Id " + fromId + " is not valid.");
    }

    if (record == null) {
      throw new GroundDbException("Id " + fromId + " is not valid.");
    }

    predicates.clear();
    predicates.add(new DbDataContainer("id", GroundType.LONG, toId));
    try {
      record = this.dbClient.getVertex(predicates);
    } catch (EmptyResultException e) {
      throw new GroundDbException("Id " + toId + " is not valid.");
    }

    if (record == null) {
      throw new GroundDbException("Id " + fromId + " is not valid.");
    }

    long dbId = idGenerator.generateSuccessorId();

    predicates.clear();
    predicates.add(new DbDataContainer("id", GroundType.LONG, dbId));
    predicates.add(new DbDataContainer("fromId", GroundType.LONG, fromId));
    predicates.add(new DbDataContainer("toId", GroundType.LONG, toId));

    this.dbClient.addEdge("VersionSuccessor", fromId, toId, predicates);

    return VersionSuccessorFactory.construct(dbId, fromId, toId);
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

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("id", GroundType.LONG, dbId));

    Relationship result;
    try {
      result = this.dbClient.getEdge("VersionSuccessor", predicates);
    } catch (EmptyResultException e) {
      throw new GroundDbException("No VersionSuccessor found with id " + dbId + ".");
    }

    return this.constructFromRelationship(result);
  }

  private <T extends Version> VersionSuccessor<T> constructFromRelationship(Relationship r) {
    long id = (r.get("id")).asLong();
    long fromId = (r.get("fromId")).asLong();
    long toId = (r.get("toId")).asLong();

    return VersionSuccessorFactory.construct(id, fromId, toId);
  }

  /**
   * Delete a version successor from the database.
   *
   * @param toId the destination version
   */
  @Override
  public void deleteFromDestination(long toId, long itemId) throws GroundException {
    // this is not currently needed and would require a special case function in the DB client
    throw new NotImplementedException();
  }
}
