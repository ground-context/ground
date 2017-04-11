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

import edu.berkeley.ground.dao.models.RichVersionFactory;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.usage.LineageEdgeVersion;

import java.util.List;
import java.util.Map;

public interface LineageEdgeVersionFactory extends RichVersionFactory<LineageEdgeVersion> {

  LineageEdgeVersion create(Map<String, Tag> tags,
                                            long structureVersionId,
                                            String reference,
                                            Map<String, String> referenceParameters,
                                            long fromId,
                                            long toId,
                                            long lineageEdgeId,
                                            List<Long> parentIds) throws GroundException;

  @Override
  default Class<LineageEdgeVersion> getType() {
    return LineageEdgeVersion.class;
  }

  @Override
  LineageEdgeVersion retrieveFromDatabase(long id) throws GroundException;
}
