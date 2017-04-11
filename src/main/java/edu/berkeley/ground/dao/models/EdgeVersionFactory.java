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

package edu.berkeley.ground.dao.models;

import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.EdgeVersion;
import edu.berkeley.ground.model.models.Tag;

import java.util.List;
import java.util.Map;

public interface EdgeVersionFactory extends RichVersionFactory<EdgeVersion> {

  EdgeVersion create(Map<String, Tag> tags,
                     long structureVersionId,
                     String reference,
                     Map<String, String> referenceParameters,
                     long edgeId,
                     long fromNodeVersionStartId,
                     long fromNodeVersionEndId,
                     long toNodeVersionStartId,
                     long toNodeVersionEndId,
                     List<Long> parentIds) throws GroundException;

  @Override
  default Class<EdgeVersion> getType() {
    return EdgeVersion.class;
  }

  @Override
  EdgeVersion retrieveFromDatabase(long id) throws GroundException;

  void updatePreviousVersion(long id, long fromEndId, long toEndId) throws GroundException;
}
