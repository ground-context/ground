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
package edu.berkeley.ground.common.factory.core;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.factory.version.ItemFactory;
import edu.berkeley.ground.common.model.core.Edge;

import java.util.List;

import play.db.Database;

public interface EdgeFactory extends ItemFactory<Edge> {

  //Edge create(String name, String sourceKey, long fromNodeId, long toNodeId, Map<String, Tag> tags)
      //throws GroundException;

  @Override
  default Class<Edge> getType() {
    return Edge.class;
  }

  @Override
  Edge retrieveFromDatabase(Database dbSource, String sourceKey) throws GroundException;

  @Override
  Edge retrieveFromDatabase(Database dbSource, long id) throws GroundException;

  List<Long> getLeaves(Database dbSource, String sourceKey) throws GroundException;
}
