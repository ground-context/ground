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

package dao.versions.cassandra;

import dao.versions.VersionSuccessorFactory;
import db.CassandraClient;
import db.CassandraResults;
import db.DbClient;
import db.DbDataContainer;
import exceptions.GroundException;
import models.versions.GroundType;
import models.versions.Version;
import models.versions.VersionSuccessor;
import util.IdGenerator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CassandraVersionSuccessorFactory implements VersionSuccessorFactory {
  private final CassandraClient dbClient;
  private final IdGenerator idGenerator;

  public CassandraVersionSuccessorFactory(CassandraClient dbClient, IdGenerator idGenerator) {
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
    // check to see if both are valid ids since we don't have foreign key constraints
    verifyVersion(fromId);
    verifyVersion(toId);

    List<DbDataContainer> insertions = new ArrayList<>();

    long dbId = this.idGenerator.generateSuccessorId();

    insertions.add(new DbDataContainer("id", GroundType.LONG, dbId));
    insertions.add(new DbDataContainer("from_version_id", GroundType.LONG, fromId));
    insertions.add(new DbDataContainer("to_version_id", GroundType.LONG, toId));

    this.dbClient.insert("version_successor", insertions);

    return new VersionSuccessor<>(dbId, toId, fromId);
  }

  public <T extends Version> List<VersionSuccessor<T>> retrieveFromDatabaseByItemId(long itemId)
    throws GroundException {
    CassandraResults resultSet;

    List<VersionSuccessor<T>> versionSuccessors = new ArrayList<>();
    resultSet = this.dbClient.selectWhereCollectionContains(
      "version_successor",
      DbClient.SELECT_STAR,
      "item_id_set",
      new DbDataContainer(null, GroundType.LONG, itemId));

    if (resultSet.isEmpty()) {
      throw new GroundException("No VersionSuccessor found with itemId " + itemId + ".");
    }

    do {
      long id = resultSet.getLong("id");
      long fromId = resultSet.getLong("from_version_id");
      long toId = resultSet.getLong("to_version_id");
      versionSuccessors.add(new VersionSuccessor<>(id, fromId, toId));
    } while (resultSet.next());

    return versionSuccessors;
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

    CassandraResults resultSet = this.dbClient.equalitySelect("version_successor",
        DbClient.SELECT_STAR,
        predicates);

    if (resultSet.isEmpty()) {
      throw new GroundException("No VersionSuccessor found with id " + dbId + ".");
    }

    long fromId = resultSet.getLong("from_version_id");
    long toId = resultSet.getLong("to_version_id");

    return new VersionSuccessor<>(dbId, fromId, toId);
  }

  /**
   * Delete a version successor from the database.
   *
   * @param toId the destination version
   */
  @Override
  public void deleteFromDestination(long toId, long itemId) throws GroundException {
    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("to_version_id", GroundType.LONG, toId));

    CassandraResults resultSet = this.dbClient.equalitySelect("version_successor",
        DbClient.SELECT_STAR,
        predicates);

    if (resultSet.isEmpty()) {
      throw new GroundException("Version " + toId + " was not part of a DAG.");
    }

    do {
      long dbId = resultSet.getLong("id");

      predicates.clear();
      predicates.add(new DbDataContainer("id", GroundType.LONG, dbId));
      Set<Long> value = new HashSet<>();
      value.add(itemId);

      this.dbClient.deleteFromSet("version_successor", "item_id_set", value, predicates);

      predicates.clear();
      predicates.add(new DbDataContainer("id", GroundType.LONG, dbId));

      this.dbClient.delete(predicates, "version_successor");
    } while (resultSet.next());
  }

  private void verifyVersion(long id) throws GroundException {
    List<DbDataContainer> predicate = new ArrayList<>();
    predicate.add(new DbDataContainer("id", GroundType.LONG, id));

    CassandraResults resultSet = this.dbClient.equalitySelect("version", DbClient.SELECT_STAR,
        predicate);

    if (resultSet.isEmpty()) {
      throw new GroundException("Version id " + id + " is not valid.");
    }
  }
}
