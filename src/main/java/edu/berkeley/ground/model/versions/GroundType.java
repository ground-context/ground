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

import java.sql.Types;

public enum GroundType {
  STRING(String.class, "string", Types.VARCHAR),
  INTEGER(Integer.class, "integer", Types.INTEGER),
  BOOLEAN(Boolean.class, "boolean", Types.BOOLEAN),
  LONG(Long.class, "long", Types.BIGINT);

  private final Class<?> klass;
  private final String name;
  private final int sqlType;

  GroundType(Class<?> klass, String name, int sqlType) {
    this.klass = klass;
    this.name = name;
    this.sqlType = sqlType;
  }

  /**
   * Returns the SQL type as defined in java.sql.Types corresponding to this GroundType
   * @return an integer the corresponding SQL Type
   */
  public int getSqlType(){
    return sqlType;
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
        // impossible because we've listed all enum values
        throw new IllegalStateException("Unhandled enum value: " + groundType);
    }
  }

  @Override
  public String toString() {
    return this.name;
  }
}
