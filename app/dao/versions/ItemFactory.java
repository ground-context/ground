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

import db.DbResults;
import exceptions.GroundException;
import exceptions.GroundItemExistsException;
import exceptions.GroundItemNotFoundException;
import models.versions.Item;

import java.util.List;

public interface ItemFactory<T extends Item> {
  T retrieveFromDatabase(long id) throws GroundException;

  T retrieveFromDatabase(String sourceKey) throws GroundException;

  Class<T> getType();

  List<Long> getLeaves(String sourceKey) throws GroundException;

  /**
   * Add a new Version to this Item. The provided parentIds will be the parents of this particular
   * version. What's provided in the default case varies based on which database we are writing
   * into.
   *
   * @param itemId the id of the Item we're updating
   * @param childId the new version's id
   * @param parentIds the ids of the parents of the child
   */
  void update(long itemId, long childId, List<Long> parentIds) throws GroundException;

  /**
   * Truncate the item to only have the most recent levels.
   *
   * @param numLevels the levels to keep
   * @throws GroundException an error while removing versions
   */
  void truncate(long itemId, int numLevels) throws GroundException;

  default boolean checkIfItemExists(String sourceKey) throws GroundException {
    try {
      this.retrieveFromDatabase(sourceKey);

      return true;
    } catch (GroundItemNotFoundException e) {
      return false;
    }
  }

  default void verifyItemNotExists(String sourceKey) throws GroundException {
    if (checkIfItemExists(sourceKey)) {
      throw new GroundItemExistsException(getType(), sourceKey);
    }
  }

  /**
   * Verify that a result set for an item is not empty.
   *
   * @param resultSet the result set to check
   * @param fieldName the name of the field that was used to retrieve this item
   * @param value the value used to retrieve the item
   * @throws GroundItemNotFoundException an exception indicating the item wasn't found
   */
  default void verifyResultSet(DbResults resultSet, String fieldName, Object value)
    throws GroundException {

    if (resultSet.isEmpty()) {
      throw new GroundItemNotFoundException(this.getType(), fieldName, value);
    }
  }
}
