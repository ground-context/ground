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

import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.Map;

public abstract class RichVersionFactory {
  public abstract void insertIntoDatabase(GroundDBConnection connection,
                                          long id,
                                          Map<String, Tag> tags,
                                          long structureVersionId,
                                          String reference,
                                          Map<String, String> referenceParameters) throws GroundException;

  public abstract RichVersion retrieveFromDatabase(GroundDBConnection connection, long id) throws GroundException;

  protected static RichVersion construct(long id,
                                         Map<String, Tag> tags,
                                         long structureVersionId,
                                         String reference,
                                         Map<String, String> parameters) {
    return new RichVersion(id, tags, structureVersionId, reference, parameters);
  }

  /**
   * Validate that the given Tags satisfy the StructureVersion's requirements.
   *
   * @param structureVersion the StructureVersion to check against
   * @param tags             the provided tags
   */
  protected static void checkStructureTags(StructureVersion structureVersion, Map<String, Tag> tags) throws GroundException {
    Map<String, GroundType> structureVersionAttributes = structureVersion.getAttributes();

    if (tags.isEmpty()) {
      throw new GroundException("No tags were specified");
    }

    for (String key : structureVersionAttributes.keySet()) {
      // check if such a tag exists
      if (!tags.keySet().contains(key)) {
        throw new GroundException("No tag with key " + key + " was specified.");
      } else if (tags.get(key).getValueType() == null) { // check that value type is specified
        throw new GroundException("Tag with key " + key + " did not have a value.");
      } else if (!tags.get(key).getValueType().equals(structureVersionAttributes.get(key))) { // check that the value type is the same
        throw new GroundException("Tag with key " + key + " did not have a value of the correct type.");
      }
    }
  }
}
