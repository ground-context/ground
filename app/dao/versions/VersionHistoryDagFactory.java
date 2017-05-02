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

package dao.versions;

import exceptions.GroundException;
import models.versions.Item;
import models.versions.Version;
import models.versions.VersionHistoryDag;

public interface VersionHistoryDagFactory {
  <T extends Version> VersionHistoryDag<T> create(long itemId) throws GroundException;

  <T extends Version> VersionHistoryDag<T> retrieveFromDatabase(long itemId) throws GroundException;

  /**
   * Add a new edge between parentId and childId in DAG.
   *
   * @param dag the DAG to update
   * @param parentId the parent's id
   * @param childId the child's id
   * @param itemId the id of the Item whose DAG we're updating
   */
  <T extends Version> void addEdge(VersionHistoryDag<T> dag, long parentId, long childId, long itemId)
      throws GroundException;

  /**
   * Truncate the DAG to only have a certain number of levels, removing everything before that.
   *
   * TODO: Once we have delta-encoded tags, this should also update the associated tags.
   *
   * @param dag the DAG to truncate
   * @param numLevels the number of levels to keep
   */
  <T extends Version> void truncate(VersionHistoryDag<T> dag,
                                    int numLevels,
                                    Class<? extends Item> itemType) throws GroundException;
}
