/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.ground.common.dao.version;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.version.Item;
import edu.berkeley.ground.common.model.version.VersionHistoryDag;
import edu.berkeley.ground.common.util.DbStatements;

public interface VersionHistoryDagDao {

  VersionHistoryDag create(long itemId) throws GroundException;

  VersionHistoryDag retrieveFromDatabase(long itemId) throws GroundException;

  /**
   * Add a new edge between parentId and childId in DAG.
   *
   * @param dag the DAG to update
   * @param parentId the parent's id
   * @param childId the child's id
   * @param itemId the id of the Item whose DAG we're updating
   */
  DbStatements<?> addEdge(VersionHistoryDag dag, long parentId, long childId, long itemId) throws GroundException;

  /**
   * Truncate the DAG to only have a certain number of levels, removing everything before that.
   *
   * TODO: Once we have delta-encoded tags, this should also update the associated tags.
   *
   * @param dag the DAG to truncate
   * @param numLevels the number of levels to keep
   */
  void truncate(VersionHistoryDag dag, int numLevels, Class<? extends Item> itemType) throws GroundException;
}
