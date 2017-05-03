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
package edu.berkeley.ground.lib.model.core;

import edu.berkeley.ground.lib.model.version.Tag;
import edu.berkeley.ground.lib.model.version.Version;
import java.util.Map;
import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RichVersion extends Version {
  private long id;

  // the map of Keys to Tags associated with this RichVersion
  private final Map<String, Tag> tags;

  // the StructureVersion associated with this RichVersion
  private final long structureVersionId;

  // the optional reference associated with this RichVersion
  private final String reference;

  // the optional parameters associated with this RichVersion if there is a reference
  private final Map<String, String> parameters;

  /**
   * Create a new rich version.
   *
   * @param id the id of the version
   * @param tags the tags associated with this version
   * @param structureVersionId the id of the StructureVersion associated with this version
   * @param reference an optional external reference
   * @param referenceParameters the access parameters for the reference
   */
  @JsonCreator
  public RichVersion(
      long id,
      Map<String, Tag> tags,
      long structureVersionId,
      String reference,
      Map<String, String> referenceParameters) {

    super(id);
    this.id = id;
    this.tags = tags;
    this.structureVersionId = structureVersionId;
    this.reference = reference;
    this.parameters = referenceParameters;
  }

  public long getId() {
    return this.id;
  }

  public Map<String, Tag> getTags() {
    return this.tags;
  }

  public long getStructureVersionId() {
    return this.structureVersionId;
  }

  public String getReference() {
    return this.reference;
  }

  public Map<String, String> getParameters() {
    return this.parameters;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof RichVersion)) {
      return false;
    }

    RichVersion otherRichVersion = (RichVersion) other;

    return this.getId() == otherRichVersion.getId()
        && this.tags.equals(otherRichVersion.tags)
        && this.structureVersionId == otherRichVersion.structureVersionId
        && Objects.equals(this.reference, otherRichVersion.reference)
        && Objects.equals(this.parameters, otherRichVersion.parameters);
  }
}
