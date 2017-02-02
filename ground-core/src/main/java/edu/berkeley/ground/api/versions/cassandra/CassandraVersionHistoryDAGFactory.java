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

import edu.berkeley.ground.api.versions.*;
import edu.berkeley.ground.db.CassandraClient.CassandraConnection;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.QueryResults;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.ArrayList;
import java.util.List;

public class CassandraVersionHistoryDAGFactory extends VersionHistoryDAGFactory {
  private CassandraVersionSuccessorFactory versionSuccessorFactory;

  public CassandraVersionHistoryDAGFactory(CassandraVersionSuccessorFactory versionSuccessorFactory) {
    this.versionSuccessorFactory = versionSuccessorFactory;
  }

  public <T extends Version> VersionHistoryDAG<T> create(String itemId) throws GroundException {
    return construct(itemId);
  }

  public <T extends Version> VersionHistoryDAG<T> retrieveFromDatabase(GroundDBConnection connectionPointer, String itemId) throws GroundException {
    CassandraConnection connection = (CassandraConnection) connectionPointer;

    List<DbDataContainer> predicates = new ArrayList<>();
    predicates.add(new DbDataContainer("item_id", GroundType.STRING, itemId));
    QueryResults resultSet;
    try {
      resultSet = connection.equalitySelect("VersionHistoryDAGs", DBClient.SELECT_STAR, predicates);
    } catch (EmptyResultException eer) {
      // do nothing' this just means that no versions have been added yet.
      return VersionHistoryDAGFactory.construct(itemId, new ArrayList<VersionSuccessor<T>>());
    }

    List<VersionSuccessor<T>> edges = new ArrayList<>();

    while (resultSet.next()) {
      edges.add(this.versionSuccessorFactory.retrieveFromDatabase(connection, resultSet.getString(1)));
    }

    return VersionHistoryDAGFactory.construct(itemId, edges);
  }

  public void addEdge(GroundDBConnection connectionPointer, VersionHistoryDAG dag, String parentId, String childId, String itemId) throws GroundException {
    CassandraConnection connection = (CassandraConnection) connectionPointer;

    VersionSuccessor successor = this.versionSuccessorFactory.create(connection, parentId, childId);

    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("item_id", GroundType.STRING, itemId));
    insertions.add(new DbDataContainer("successor_id", GroundType.STRING, successor.getId()));

    connection.insert("VersionHistoryDAGs", insertions);

    dag.addEdge(parentId, childId, successor.getId());
  }
}
