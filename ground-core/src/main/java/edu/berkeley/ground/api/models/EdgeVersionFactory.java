/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
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
import java.util.Optional;

public abstract class EdgeVersionFactory {
    public abstract EdgeVersion create(Optional<Map<String, Tag>> tags,
                                       Optional<String> structureVersionId,
                                       Optional<String> reference,
                                       Optional<Map<String, String>> parameters,
                                       String edgeId,
                                       String fromId,
                                       String toId,
                                       List<String> parentIds) throws GroundException;


    public abstract EdgeVersion retrieveFromDatabase(String id) throws GroundException;

    protected static EdgeVersion construct(String id,
                                           Optional<Map<String, Tag>> tags,
                                           Optional<String> structureVersionId,
                                           Optional<String> reference,
                                           Optional<Map<String, String>> parameters,
                                           String edgeId,
                                           String fromId,
                                           String toId) throws GroundException {

        return new EdgeVersion(id, tags, structureVersionId, reference, parameters, edgeId, fromId, toId);
    }
}
