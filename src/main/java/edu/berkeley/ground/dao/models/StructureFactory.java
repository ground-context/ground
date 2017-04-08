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

package edu.berkeley.ground.dao.models;

import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.Structure;
import edu.berkeley.ground.model.models.Tag;

import java.util.List;
import java.util.Map;

public abstract class StructureFactory {
  public abstract Structure create(String name,
                                   String sourceKey,
                                   Map<String, Tag> tags)
      throws GroundException;

  public abstract Structure retrieveFromDatabase(String name) throws GroundException;

  public abstract void update(long itemId, long childId, List<Long> parentIds)
      throws GroundException;

  public abstract void truncate(long itemId, int numLevels) throws GroundException;

  public abstract List<Long> getLeaves(String name) throws GroundException;

  protected static Structure construct(long id,
                                       String name,
                                       String sourceKey,
                                       Map<String, Tag> tags) {
    return new Structure(id, name, sourceKey, tags);
  }
}
