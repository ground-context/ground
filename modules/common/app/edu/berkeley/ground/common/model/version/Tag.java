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
package edu.berkeley.ground.common.model.version;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.exception.GroundException.ExceptionType;
import java.util.Objects;

public class Tag {

  @JsonProperty("id")
  private long id;

  @JsonProperty("key")
  private final String key;

  @JsonProperty("value")
  private final Object value;

  @JsonProperty("type")
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
  public Tag(@JsonProperty("item_id") long id, @JsonProperty("key") String key, @JsonProperty("value") Object value,
              @JsonProperty("type") GroundType valueType)
    throws edu.berkeley.ground.common.exception.GroundException {

    if (!((value != null) == (valueType != null))
          || (value != null && !(value.getClass().equals(valueType.getTypeClass())))) {

      throw new GroundException(ExceptionType.OTHER, "Mismatch between value (" + value + ") and given type (" + valueType.toString() + ").");
    }

    if (value != null) {
      assert (value.getClass().equals(valueType.getTypeClass()));
    }

    this.id = id;
    this.key = key;
    this.value = value;
    this.valueType = valueType;
  }

  public long getId() {
    return this.id;
  }

  public String getKey() {
    return this.key;
  }

  public Object getValue() {
    return this.value;
  }

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
