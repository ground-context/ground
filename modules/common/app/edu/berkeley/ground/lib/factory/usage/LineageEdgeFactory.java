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
import edu.berkeley.ground.lib.model.usage.LineageEdge;
import edu.berkeley.ground.lib.model.version.Tag;
import java.util.List;
import java.util.Map;

import edu.berkeley.ground.lib.utils.IdGenerator;
import play.db.Database;

public interface LineageEdgeFactory extends ItemFactory<LineageEdge> {

  LineageEdge create(Database dbSource, LineageEdge lineageEdge) throws GroundException;

  @Override
  default Class<LineageEdge> getType() {
    return LineageEdge.class;
  }

  @Override
  LineageEdge retrieveFromDatabase(Database dbSource, String sourceKey) throws GroundException;

  @Override
  LineageEdge retrieveFromDatabase(Database dbSource, long id) throws GroundException;

  List<Long> getLeaves(Database dbSource, String sourceKey) throws GroundException;
}
