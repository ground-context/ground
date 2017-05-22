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
package edu.berkeley.ground.common.dao.usage;

import edu.berkeley.ground.common.dao.core.RichVersionDao;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.usage.LineageEdgeVersion;
import java.util.List;

public interface LineageEdgeVersionDao extends RichVersionDao<LineageEdgeVersion> {

  @Override
  LineageEdgeVersion create(LineageEdgeVersion lineageEdgeVersion, List<Long> parentIds) throws GroundException;

  @Override
  LineageEdgeVersion retrieveFromDatabase(long id) throws GroundException;

  @Override
  default Class<LineageEdgeVersion> getType() {
    return LineageEdgeVersion.class;
  }
}
