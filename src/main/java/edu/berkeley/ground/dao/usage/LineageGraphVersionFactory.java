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

import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.usage.LineageGraphVersion;

import java.util.List;
import java.util.Map;

public abstract class LineageGraphVersionFactory {
  public abstract LineageGraphVersion create(Map<String, Tag> tags,
                                             long structureVersionId,
                                             String reference,
                                             Map<String, String> referenceParameters,
                                             long lineageGraphId,
                                             List<Long> lineageEdgeVersionIds,
                                             List<Long> parentIds) throws GroundException;

  public abstract LineageGraphVersion retrieveFromDatabase(long id) throws GroundException;

  protected static LineageGraphVersion construct(long id,
                                          Map<String, Tag> tags,
                                          long structureVersionId,
                                          String reference,
                                          Map<String, String> parameters,
                                          long lineageGraphId,
                                          List<Long> lineageEdgeVersionIds) {

    return new LineageGraphVersion(id, tags, structureVersionId, reference, parameters,
        lineageGraphId, lineageEdgeVersionIds);
  }
}
