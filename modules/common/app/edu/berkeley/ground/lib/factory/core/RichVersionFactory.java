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
package edu.berkeley.ground.lib.factory.core;

import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.factory.version.VersionFactory;
import edu.berkeley.ground.lib.model.core.RichVersion;
import edu.berkeley.ground.lib.model.core.StructureVersion;
import edu.berkeley.ground.lib.model.version.GroundType;
import edu.berkeley.ground.lib.model.version.Tag;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public interface RichVersionFactory<T extends RichVersion> extends VersionFactory<T> {

  void insertIntoDatabase(
      long id,
      Map<String, Tag> tags,
      long structureVersionId,
      String reference,
      Map<String, String> referenceParameters)
      throws GroundException;

  static Map<String, Tag> addIdToTags(long id, Map<String, Tag> tags) throws GroundException {

    Function<Tag, Tag> addId =
        (Tag t) -> {
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
      throw new GroundException("No tags were specified");
    }

    for (String key : structureVersionAttributes.keySet()) {
      if (!tags.keySet().contains(key)) {
        // check if such a tag exists
        throw new GroundException("No tag with key " + key + " was specified.");
      } else if (tags.get(key).getValueType() == null) {
        // check that value type is specified
        throw new GroundException("Tag with key " + key + " did not have a value.");
      } else if (!tags.get(key).getValueType().equals(structureVersionAttributes.get(key))) {
        // check that the value type is the same
        throw new GroundException(
            "Tag with key "
                + key
                + " did not have a value of the correct type: expected ["
                + structureVersionAttributes.get(key)
                + "] but found ["
                + tags.get(key).getValueType()
                + "].");
      }
    }
  }
}
