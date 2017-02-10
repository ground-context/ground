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

package edu.berkeley.ground.api.versions;

import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.List;

public abstract class ItemFactory {
  public abstract void insertIntoDatabase(GroundDBConnection connection, long id) throws GroundException;

  /**
   * Add a new Version to this Item. The provided parentIds will be the parents of this particular
   * version. What's provided in the default case varies based on which database we are writing
   * into.
   *
   * @param itemId    the id of the Item we're updating
   * @param childId   the new version's id
   * @param parentIds the ids of the parents of the child
   */
  public abstract void update(GroundDBConnection connection, long itemId, long childId, List<Long> parentIds) throws GroundException;
}
