/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.api.models;

import edu.berkeley.ground.exceptions.GroundException;

import java.util.List;
import java.util.Map;

public abstract class NodeVersionFactory {
  public abstract NodeVersion create(Map<String, Tag> tags,
                                     long structureVersionId,
                                     String reference,
                                     Map<String, String> referenceParameters,
                                     long nodeId,
                                     List<Long> parentIds) throws GroundException;

  public abstract NodeVersion retrieveFromDatabase(long id) throws GroundException;

  public abstract List<Long> getAdjacentNodes(long nodeVersionId, String edgeNameLike) throws GroundException;

  public static NodeVersion construct(long id,
                                      Map<String, Tag> tags,
                                      long structureVersionId,
                                      String reference,
                                      Map<String, String> parameters,
                                      long nodeId) {

    return new NodeVersion(id, tags, structureVersionId, reference, parameters, nodeId);
  }
}
