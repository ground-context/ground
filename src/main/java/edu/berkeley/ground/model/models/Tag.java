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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.berkeley.ground.model.versions.GroundType;

import org.hibernate.validator.constraints.NotEmpty;
import org.hibernate.validator.valuehandling.UnwrapValidatedValue;

import java.util.Objects;

public class Tag {
  private final long id;

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
   * @param id the id of the version containing this tag
   * @param key the key of the tag
   * @param value the value of the tag
   * @param valueType the type of the value
   */
  @JsonCreator
  public Tag(@JsonProperty("id") long id,
             @JsonProperty("key") String key,
             @JsonProperty("value") Object value,
             @JsonProperty("type") GroundType valueType) {

    assert ((value != null) == (valueType != null));

    if (value != null) {
      assert (value.getClass().equals(valueType.getTypeClass()));
    }


    this.id = id;
    this.key = key;
    this.value = value;
    this.valueType = valueType;
  }

  @JsonProperty
  public long getId() {
    return this.id;
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

    return this.key.equals(otherTag.key)
        && Objects.equals(this.value, otherTag.value)
        && Objects.equals(this.valueType, otherTag.valueType);
  }
}
