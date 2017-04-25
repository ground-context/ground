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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import exceptions.GroundException;
import models.versions.GroundType;

import org.hibernate.validator.constraints.NotEmpty;
import org.hibernate.validator.valuehandling.UnwrapValidatedValue;

import java.util.Objects;

public class Tag {
  // the first RichVersion that this Tag applies to
  private final long startId;

  // the last RichVersion that this Tag applies to
  private final long endId;

  @NotEmpty
  // the Key of the Tag
  private final String key;

  @UnwrapValidatedValue
  // the optional Value of the Tag
  private final Object value;

  @UnwrapValidatedValue
  // the Type of the Value if it exists
  private final GroundType valueType;

  /**
   * Create a new tag.
   *
   * @param startId the id of the first version containing this tag
   * @param endId the id of the last version containing this tag
   * @param key the key of the tag
   * @param value the value of the tag
   * @param valueType the type of the value
   */
  @JsonCreator
  public Tag(@JsonProperty("fromRichVersionId") long startId,
             @JsonProperty("toRichVersionId") long endId,
             @JsonProperty("key") String key,
             @JsonProperty("value") Object value,
             @JsonProperty("type") GroundType valueType) throws GroundException {

    if (!((value != null) == (valueType != null)) || (value != null && !(value.getClass().equals
        (valueType.getTypeClass())))) {

      throw new GroundException("Mismatch between value (" + value + ") and given type (" +
          valueType.toString() + ").");
    }

    if (value != null) {
      assert (value.getClass().equals(valueType.getTypeClass()));
    }

    this.startId = startId;
    this.endId = endId;
    this.key = key;
    this.value = value;
    this.valueType = valueType;
  }

  @JsonProperty
  public long getStartId() {
    return this.startId;
  }

  @JsonProperty
  public long getEndId() {
    return this.endId;
  }

  @JsonProperty
  public String getKey() {
    return this.key;
  }

  @JsonProperty
  public Object getValue() {
    return this.value;
  }

  @JsonProperty
  public GroundType getValueType() {
    return this.valueType;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Tag)) {
      return false;
    }

    Tag otherTag = (Tag) other;

    return this.startId == otherTag.startId
        && this.endId == otherTag.endId
        && this.key.equals(otherTag.key)
        && Objects.equals(this.value, otherTag.value)
        && Objects.equals(this.valueType, otherTag.valueType);
  }
}
