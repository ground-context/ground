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

package dao.models;

import dao.versions.VersionFactory;
import exceptions.GroundDbException;
import edu.berkeley.ground.exception.GroundException;
import java.util.function.Function;
import models.models.RichVersion;
import models.models.StructureVersion;
import edu.berkeley.ground.model.version.Tag;
import models.versions.GroundType;

import java.util.Map;
import java.util.stream.Collectors;

public interface RichVersionFactory<T extends RichVersion> extends VersionFactory<T> {

  void insertIntoDatabase(long id,
                          Map<String, Tag> tags,
                          long structureVersionId,
                          String reference,
                          Map<String, String> referenceParameters)
      throws GroundException;

  static Map<String, Tag> addIdToTags(long id, Map<String, Tag> tags) throws GroundException {

    Function<Tag, Tag> addId = (Tag t) -> {
      try {
        return new Tag(id, t.getKey(), t.getValue(), t.getValueType());
      } catch (GroundException e) {
        throw new RuntimeException(e);
      }
    };

    return tags.values().stream().collect(Collectors.toMap(Tag::getKey, addId));
  }

  /**
   * Validate that the given Tags satisfy the StructureVersion's requirements.
   *
   * @param structureVersion the StructureVersion to check against
   * @param tags the provided tags
   */
  static void checkStructureTags(StructureVersion structureVersion, Map<String, Tag> tags)
      throws GroundException {

    Map<String, GroundType> structureVersionAttributes = structureVersion.getAttributes();

    if (tags.isEmpty()) {
      throw new GroundDbException("No tags were specified");
    }

    for (String key : structureVersionAttributes.keySet()) {
      if (!tags.keySet().contains(key)) {
        // check if such a tag exists
        throw new GroundDbException("No tag with key " + key + " was specified.");
      } else if (tags.get(key).getValueType() == null) {
        // check that value type is specified
        throw new GroundDbException("Tag with key " + key + " did not have a value.");
      } else if (!tags.get(key).getValueType().equals(structureVersionAttributes.get(key))) {
        // check that the value type is the same
        throw new GroundDbException("Tag with key "
            + key
            + " did not have a value of the correct type: expected [" +
            structureVersionAttributes.get(key) +
            "] but found [" +
            tags.get(key).getValueType() +
            "].");
      }
    }
  }
}
