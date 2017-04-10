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

package edu.berkeley.ground.model.models;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.berkeley.ground.model.versions.Version;

import java.util.Map;
import java.util.Objects;

import org.hibernate.validator.valuehandling.UnwrapValidatedValue;

public class RichVersion extends Version {
  // the map of Keys to Tags associated with this RichVersion
  private final Map<String, Tag> tags;

  // the StructureVersion associated with this RichVersion
  private final long structureVersionId;

  @UnwrapValidatedValue
  // the optional reference associated with this RichVersion
  private final String reference;

  @UnwrapValidatedValue
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
  public RichVersion(long id,
                     Map<String, Tag> tags,
                     long structureVersionId,
                     String reference,
                     Map<String, String> referenceParameters) {

    super(id);

    this.tags = tags;
    this.structureVersionId = structureVersionId == 0 ? -1 : structureVersionId;
    this.reference = reference;
    this.parameters = referenceParameters;
  }

  @JsonProperty
  public Map<String, Tag> getTags() {
    return this.tags;
  }

  @JsonProperty
  public long getStructureVersionId() {
    return this.structureVersionId;
  }

  @JsonProperty
  public String getReference() {
    return this.reference;
  }

  @JsonProperty
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
