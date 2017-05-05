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
package edu.berkeley.ground.lib.factory.usage;

import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.factory.version.ItemFactory;
import edu.berkeley.ground.lib.model.usage.LineageGraph;
import edu.berkeley.ground.lib.model.version.Tag;
import java.util.List;
import java.util.Map;
import play.db.Database;

public interface LineageGraphFactory extends ItemFactory<LineageGraph> {

  LineageGraph create(Database dbSource, LineageGraph lineageGraph) throws GroundException;

  @Override
  default Class<LineageGraph> getType() {
    return LineageGraph.class;
  }

  @Override
  LineageGraph retrieveFromDatabase(Database dbSource, String sourceKey) throws GroundException;

  @Override
  LineageGraph retrieveFromDatabase(Database dbSource, long id) throws GroundException;

  @Override
  void update(long itemId, long childId, List<Long> parentIds) throws GroundException;

  @Override
  List<Long> getLeaves(String sourceKey) throws GroundException;
}
