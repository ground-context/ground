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
package edu.berkeley.ground.lib.model.version;

import edu.berkeley.ground.lib.exception.GroundException;
import java.util.Objects;

public class Tag {
  private final long id;

  private final String key;

  private final Object value;

  private final GroundType valueType;

  /**
   * Create a new tag.
   *
   * @param id the id of the version containing this tag
   * @param key the key of the tag
   * @param value the value of the tag
   * @param valueType the type of the value
   */
  public Tag(long id, String key, Object value, GroundType valueType)
      throws edu.berkeley.ground.lib.exception.GroundException {

    if (!((value != null) == (valueType != null))
        || (value != null && !(value.getClass().equals(valueType.getTypeClass())))) {

      throw new GroundException(
          "Mismatch between value (" + value + ") and given type (" + valueType.toString() + ").");
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
