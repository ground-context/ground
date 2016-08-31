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

import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.Map;
import java.util.Optional;

public abstract class RichVersionFactory {
    public abstract void insertIntoDatabase(GroundDBConnection connection, String id, Optional<Map<String, Tag>>tags, Optional<String> structureVersionId, Optional<String> reference, Optional<Map<String, String>> parameters) throws GroundException;

    public abstract RichVersion retrieveFromDatabase(GroundDBConnection connection, String id) throws GroundException;

    protected static RichVersion construct(String id, Optional<Map<String, Tag>>tags, Optional<String> structureVersionId, Optional<String> reference, Optional<Map<String, String>> parameters) {
        return new RichVersion(id, tags, structureVersionId, reference, parameters);
    }

    protected static void checkStructureTags(StructureVersion structureVersion, Optional<Map<String, Tag>> tags) throws GroundException {
        Map<String, GroundType> structureVersionAttributes = structureVersion.getAttributes();

        if(!tags.isPresent()) {
            throw new GroundException("No tags were specified");
        }

        Map<String, Tag> tagsMap = tags.get();

        for (String key : structureVersionAttributes.keySet()) {
            // check if such a tag exists
            if(!tagsMap.keySet().contains(key)) {
                throw new GroundException("No tag with key " + key + " was specified.");
            } else if (!tagsMap.get(key).getValueType().isPresent()) { // check that value type is specified
                throw new GroundException("Tag with key " + key + " did not have a value.");
            } else if (!tagsMap.get(key).getValueType().get().equals(structureVersionAttributes.get(key))) { // check that the value type is the same
                throw new GroundException("Tag with key " + key + " did not have a value of the correct type.");
            }
        }
    }
}
