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

package edu.berkeley.ground.dao.usage;

import java.util.List;
import java.util.Map;

import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.usage.LineageGraph;

public abstract class LineageGraphFactory {
  public abstract LineageGraph create(String name,
                                      String sourceKey,
                                      Map<String, Tag> tags)
      throws GroundException;

  public abstract LineageGraph retrieveFromDatabase(String name) throws GroundException;

  public abstract void update(long itemId, long childId, List<Long> parentIds) throws GroundException;

  protected static LineageGraph construct(long id,
                                          String name,
                                          String sourceKey,
                                          Map<String, Tag> tags) {
    return new LineageGraph(id, name, sourceKey, tags);
  }
}
