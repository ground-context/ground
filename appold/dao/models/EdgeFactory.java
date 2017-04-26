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
package dao.models;

import dao.versions.ItemFactory;
import edu.berkeley.ground.exception.GroundException;
import edu.berkeley.ground.model.version.Tag;
import java.util.List;
import java.util.Map;
import models.models.Edge;

public interface EdgeFactory extends ItemFactory<Edge> {

  Edge create(String name, String sourceKey, long fromNodeId, long toNodeId, Map<String, Tag> tags)
      throws GroundException;

  @Override
  default Class<Edge> getType() {
    return Edge.class;
  }

  @Override
  Edge retrieveFromDatabase(String sourceKey) throws GroundException;

  @Override
  Edge retrieveFromDatabase(long id) throws GroundException;

  @Override
  void update(long itemId, long childId, List<Long> parentIds) throws GroundException;

  @Override
  List<Long> getLeaves(String sourceKey) throws GroundException;
}
