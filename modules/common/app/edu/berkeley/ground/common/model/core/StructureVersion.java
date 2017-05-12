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
package edu.berkeley.ground.common.model.core;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.berkeley.ground.common.model.version.GroundType;
import edu.berkeley.ground.common.model.version.Version;
import java.util.Map;

public class StructureVersion extends Version {
  // the id of the Structure containing this Version
  @JsonProperty("structureId")
  private final long structureId;

  // the map of attribute names to types
  @JsonProperty("attributes")
  private final Map<String, GroundType> attributes;

  /**
   * Create a new structure version.
   *
   * @param id the id of the structure version
   * @param structureId the id of the structure containing this version
   * @param attributes the attributes required by this structure version
   */
  public StructureVersion(
      @JsonProperty("id") long id,
      @JsonProperty("structureId") long structureId,
      @JsonProperty("attributes") Map<String, GroundType> attributes) {
    super(id);

    this.structureId = structureId;
    this.attributes = attributes;
  }

  public long getStructureId() {
    return this.structureId;
  }

  public Map<String, GroundType> getAttributes() {
    return this.attributes;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof StructureVersion)) {
      return false;
    }

    StructureVersion otherStructureVersion = (StructureVersion) other;

    return this.structureId == otherStructureVersion.structureId
        && this.attributes.equals(otherStructureVersion.attributes)
        && this.getId() == otherStructureVersion.getId();
  }
}
