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

import edu.berkeley.ground.api.versions.*;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.db.QueryResults;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.ArrayList;
import java.util.List;

public class PostgresVersionHistoryDAGFactory extends VersionHistoryDAGFactory {
  private final PostgresClient dbClient;
  private final PostgresVersionSuccessorFactory versionSuccessorFactory;

  public PostgresVersionHistoryDAGFactory(PostgresClient dbClient,
                                          PostgresVersionSuccessorFactory versionSuccessorFactory) {
    this.dbClient = dbClient;
    this.versionSuccessorFactory = versionSuccessorFactory;
  }

  public <T extends Version> VersionHistoryDAG<T> create(long itemId) throws GroundException {
    return construct(itemId);
  }

  public <T extends Version> VersionHistoryDAG<T> retrieveFromDatabase(long itemId) throws GroundException {
    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("item_id", GroundType.LONG, itemId));

    QueryResults resultSet;
    try {
      resultSet = this.dbClient.equalitySelect("version_history_dag", DBClient.SELECT_STAR, predicates);
    } catch (EmptyResultException e) {
      // do nothing' this just means that no versions have been added yet.
      return VersionHistoryDAGFactory.construct(itemId, new ArrayList<VersionSuccessor<T>>());
    }

    List<VersionSuccessor<T>> edges = new ArrayList<>();
    do {
      edges.add(this.versionSuccessorFactory.retrieveFromDatabase(resultSet.getLong(2)));
    } while (resultSet.next());

    return VersionHistoryDAGFactory.construct(itemId, edges);
  }

  public void addEdge(VersionHistoryDAG dag, long parentId, long childId, long itemId) throws GroundException {
    VersionSuccessor successor = this.versionSuccessorFactory.create(parentId, childId);

    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("item_id", GroundType.LONG, itemId));
    insertions.add(new DbDataContainer("version_successor_id", GroundType.LONG, successor.getId()));

    this.dbClient.insert("version_history_dag", insertions);

    dag.addEdge(parentId, childId, successor.getId());
  }
}
