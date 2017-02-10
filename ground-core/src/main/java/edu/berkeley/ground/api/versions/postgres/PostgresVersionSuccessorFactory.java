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

package edu.berkeley.ground.api.versions.postgres;

import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.api.versions.Version;
import edu.berkeley.ground.api.versions.VersionSuccessor;
import edu.berkeley.ground.api.versions.VersionSuccessorFactory;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.PostgresClient.PostgresConnection;
import edu.berkeley.ground.db.QueryResults;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;

import java.util.ArrayList;
import java.util.List;

public class PostgresVersionSuccessorFactory extends VersionSuccessorFactory {
  private IdGenerator idGenerator;

  public PostgresVersionSuccessorFactory(IdGenerator idGenerator) {
    this.idGenerator = idGenerator;
  }

  public <T extends Version> VersionSuccessor<T> create(GroundDBConnection connectionPointer, long fromId, long toId) throws GroundException {
    PostgresConnection connection = (PostgresConnection) connectionPointer;

    List<DbDataContainer> insertions = new ArrayList<>();
    long dbId = idGenerator.generateSuccessorId();

    insertions.add(new DbDataContainer("id", GroundType.LONG, dbId));
    insertions.add(new DbDataContainer("from_version_id", GroundType.LONG, fromId));
    insertions.add(new DbDataContainer("to_version_id", GroundType.LONG, toId));

    connection.insert("version_successor", insertions);

    return VersionSuccessorFactory.construct(dbId, toId, fromId);

  }

  public <T extends Version> VersionSuccessor<T> retrieveFromDatabase(GroundDBConnection connectionPointer, long dbId) throws GroundException {
    PostgresConnection connection = (PostgresConnection) connectionPointer;

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("id", GroundType.LONG, dbId));

    QueryResults resultSet;
    try {
      resultSet = connection.equalitySelect("version_successor", DBClient.SELECT_STAR, predicates);
    } catch (EmptyResultException eer) {
      throw new GroundException("No VersionSuccessor found with id " + dbId + ".");
    }

    long toId = resultSet.getLong(2);
    long fromId = resultSet.getLong(3);

    return VersionSuccessorFactory.construct(dbId, toId, fromId);
  }
}
