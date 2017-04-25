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

package models.models;




import models.versions.Item;

import java.util.Map;

public class Structure extends Item<StructureVersion> {
  // the name of this Structure
  private final String name;

  // the source key for this Node
  private final String sourceKey;

  /**
   * Create a new structure.
   *
   * @param id the id of the structure
   * @param name the name of the structure
   * @param sourceKey the user-generated unique key for the structure
   * @param tags the tags associated with this structure
   */
  
  public Structure(("id") long id,
                   ("name") String name,
                   ("sourceKey") String sourceKey,
                   ("tags") Map<String, Tag> tags) {
    super(id, tags);

    this.name = name;
    this.sourceKey = sourceKey;
  }

  
  public String getName() {
    return this.name;
  }

  
  public String getSourceKey() {
    return this.sourceKey;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Structure)) {
      return false;
    }

    Structure otherStructure = (Structure) other;

    return this.name.equals(otherStructure.name)
        && this.getId() == otherStructure.getId()
        && this.sourceKey.equals(otherStructure.sourceKey)
        && this.getTags().equals(otherStructure.getTags());
  }
}
