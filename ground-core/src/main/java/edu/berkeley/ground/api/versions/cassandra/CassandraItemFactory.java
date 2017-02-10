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

import edu.berkeley.ground.api.versions.ItemFactory;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.api.versions.VersionHistoryDAG;
import edu.berkeley.ground.db.CassandraClient.CassandraConnection;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.exceptions.GroundException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CassandraItemFactory extends ItemFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraItemFactory.class);

  private CassandraVersionHistoryDAGFactory versionHistoryDAGFactory;

  public CassandraItemFactory(CassandraVersionHistoryDAGFactory versionHistoryDAGFactory) {
    this.versionHistoryDAGFactory = versionHistoryDAGFactory;
  }

  public void insertIntoDatabase(GroundDBConnection connectionPointer, long id) throws GroundException {
    CassandraConnection connection = (CassandraConnection) connectionPointer;

    List<DbDataContainer> insertions = new ArrayList<>();
    insertions.add(new DbDataContainer("id", GroundType.LONG, id));

    connection.insert("item", insertions);
  }

  // TODO: Refactor logic for parent into function in ItemFactory
  public void update(GroundDBConnection connectionPointer, long itemId, long childId, List<Long> parentIds) throws GroundException {
    // If a parent is specified, great. If it's not specified, then make it a child of EMPTY, which is version 0.
    if (parentIds.isEmpty()) {
      parentIds.add(0L);
    }

    VersionHistoryDAG dag;
    try {
      dag = this.versionHistoryDAGFactory.retrieveFromDatabase(connectionPointer, itemId);
    } catch (GroundException e) {
      if (!e.getMessage().contains("No VersionHistoryDAG for Item")) {
        throw e;
      }

      dag = this.versionHistoryDAGFactory.create(itemId);
    }

    for (long parentId : parentIds) {
      if (!(parentId == 0) && !dag.checkItemInDag(parentId)) {
        String errorString = "Parent " + parentId + " is not in Item " + itemId + ".";

        LOGGER.error(errorString);
        throw new GroundException(errorString);
      }

      this.versionHistoryDAGFactory.addEdge(connectionPointer, dag, parentId, childId, itemId);
    }
  }

  public List<Long> getLeaves(GroundDBConnection connection, long itemId) throws GroundException {
    try {
      VersionHistoryDAG<?> dag = this.versionHistoryDAGFactory.retrieveFromDatabase(connection, itemId);

      return dag.getLeaves();
    } catch (GroundException e) {
      if (!e.getMessage().contains("No results found for query:")) {
        throw e;
      }

      return new ArrayList<>();
    }
  }
}
