/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.api.versions.cassandra;

import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.api.versions.Version;
import edu.berkeley.ground.api.versions.VersionSuccessor;
import edu.berkeley.ground.api.versions.VersionSuccessorFactory;
import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.QueryResults;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;

import java.util.ArrayList;
import java.util.List;

public class CassandraVersionSuccessorFactory extends VersionSuccessorFactory {
  private final CassandraClient dbClient;
  private final IdGenerator idGenerator;

  public CassandraVersionSuccessorFactory(CassandraClient dbClient, IdGenerator idGenerator) {
    this.dbClient = dbClient;
    this.idGenerator = idGenerator;
  }

  public <T extends Version> VersionSuccessor<T> create(long fromId, long toId) throws GroundException {
    // check to see if both are valid ids since we don't have foreign key constraints
    QueryResults results;
    List<DbDataContainer> predicates = new ArrayList<>();

    predicates.add(new DbDataContainer("id", GroundType.LONG, fromId));
    try {
      results = this.dbClient.equalitySelect("version", DBClient.SELECT_STAR, predicates);
    } catch (EmptyResultException e) {
      throw new GroundException("Id " + fromId + " is not valid.");
    }

    if (!results.next()) {
      throw new GroundException("Id " + fromId + " is not valid.");
    }

    predicates.clear();
    predicates.add(new DbDataContainer("id", GroundType.LONG, toId));
    try {
      results = this.dbClient.equalitySelect("version", DBClient.SELECT_STAR, predicates);
    } catch (EmptyResultException e) {
      throw new GroundException("Id " + toId + " is not valid.");
    }
    if (!results.next()) {
      throw new GroundException("Id " + toId + " is not valid.");
    }

    List<DbDataContainer> insertions = new ArrayList<>();


    long dbId = this.idGenerator.generateSuccessorId();

    insertions.add(new DbDataContainer("id", GroundType.LONG, dbId));
    insertions.add(new DbDataContainer("from_version_id", GroundType.LONG, fromId));
    insertions.add(new DbDataContainer("to_version_id", GroundType.LONG, toId));

    this.dbClient.insert("version_successor", insertions);

    return VersionSuccessorFactory.construct(dbId, toId, fromId);
  }

  public <T extends Version> VersionSuccessor<T> retrieveFromDatabase(long dbId) throws GroundException {
    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("id", GroundType.LONG, dbId));

    QueryResults resultSet;
    try {
      resultSet = this.dbClient.equalitySelect("version_successor", DBClient.SELECT_STAR, predicates);
    } catch (EmptyResultException e) {
      throw new GroundException("No VersionSuccessor found with id " + dbId + ".");
    }

    if (!resultSet.next()) {
      throw new GroundException("No VersionSuccessor found with id " + dbId + ".");
    }

    long fromId = resultSet.getLong("from_version_id");
    long toId = resultSet.getLong("to_version_id");

    return VersionSuccessorFactory.construct(dbId, fromId, toId);
  }
}
