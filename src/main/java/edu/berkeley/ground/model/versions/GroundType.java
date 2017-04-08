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

package edu.berkeley.ground.model.versions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import edu.berkeley.ground.exceptions.GroundException;

public enum GroundType {
  STRING(String.class, "string"),
  INTEGER(Integer.class, "integer"),
  BOOLEAN(Boolean.class, "boolean"),
  LONG(Long.class, "long");

  private final Class<?> klass;
  private final String name;

  GroundType(Class<?> klass, String name) {
    this.klass = klass;
    this.name = name;
  }

  public Class<?> getTypeClass() {
    return this.klass;
  }

  /**
   * Return a type based on the string name.
   *
   * @param str the name of the type
   * @return the corresponding GroundType
   * @throws GroundException no such type
   */
  @JsonCreator
  public static GroundType fromString(String str) throws GroundException {
    if (str == null) {
      return null;
    }

    switch (str.toLowerCase()) {
      case "string":
        return STRING;
      case "integer":
        return INTEGER;
      case "boolean":
        return BOOLEAN;
      case "long":
        return LONG;

      default: {
        throw new GroundException("Invalid type: " + str + ".");
      }
    }
  }

  /**
   * Take a string of type GroundType and return the parsed object.
   *
   * @param str the value
   * @param groundType the type of the value
   * @return the parsed object
   */
  @JsonValue
  public static Object stringToType(String str, GroundType groundType) {
    if (str == null) {
      return null;
    }

    switch (groundType) {
      case STRING:
        return str;
      case INTEGER:
        return Integer.parseInt(str);
      case BOOLEAN:
        return Boolean.parseBoolean(str);
      case LONG:
        return Long.parseLong(str);

      default:
        return null;
    }
  }

  @Override
  public String toString() {
    return this.name;
  }
}
