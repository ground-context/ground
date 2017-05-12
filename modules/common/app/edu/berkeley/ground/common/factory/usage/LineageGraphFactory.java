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
package edu.berkeley.ground.common.factory.usage;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.factory.version.ItemFactory;
import edu.berkeley.ground.common.model.usage.LineageGraph;

import java.util.List;

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

  List<Long> getLeaves(Database dbSource, String sourceKey) throws GroundException;
}
