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
import edu.berkeley.ground.common.exception.GroundException.ExceptionType;
import edu.berkeley.ground.common.model.version.Item;
import edu.berkeley.ground.common.util.DbStatements;
import java.util.List;


public interface ItemDao<T extends Item> {

  T create(T item) throws GroundException;

  DbStatements insert(T item) throws GroundException;

  T retrieveFromDatabase(long id) throws GroundException;

  T retrieveFromDatabase(String sourceKey) throws GroundException;

  Class<T> getType();

  List<Long> getLeaves(long itemId) throws GroundException;

  /**
   * Add a new Version to this Item. The provided parentIds will be the parents of this particular
   * version. What's provided in the default case varies based on which database we are writing
   * into.
   *
   * @param itemId the id of the Item we're updating
   * @param childId the new version's id
   * @param parentIds the ids of the parents of the child
   */
  DbStatements update(long itemId, long childId, List<Long> parentIds) throws GroundException;

  /**
   * Truncate the item to only have the most recent levels.
   *
   * @param numLevels the levels to keep
   * @throws GroundException an error while removing versions
   */
  void truncate(long itemId, int numLevels) throws GroundException;

  default boolean checkIfItemExists(String sourceKey) {
    try {
      this.retrieveFromDatabase(sourceKey);

      return true;
    } catch (GroundException e) {
      return false;
    }
  }

  default void verifyItemNotExists(String sourceKey) throws GroundException {
    if (checkIfItemExists(sourceKey)) {
      throw new GroundException(ExceptionType.ITEM_ALREADY_EXISTS, this.getType().getSimpleName(), sourceKey);
    }
  }
}
