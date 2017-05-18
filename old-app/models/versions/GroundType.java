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

package models.versions;

import com.fasterxml.jackson.annotation.JsonCreator;

import exceptions.GroundException;

import java.sql.Types;

public enum GroundType {
  STRING(String.class, "string", Types.VARCHAR){
    public Object parse(String str){
      return str;
    }
  },
  INTEGER(Integer.class, "integer", Types.INTEGER){
    public Object parse(String str){
      return Integer.parseInt(str);
    }
  },
  BOOLEAN(Boolean.class, "boolean", Types.BOOLEAN){
    public Object parse(String str){
      return Boolean.parseBoolean(str);
    }
  },
  LONG(Long.class, "long", Types.BIGINT){
    public Object parse(String str){
      return Long.parseLong(str);
    }
  };

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

  public abstract Object parse(String str);

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
    try {
      return GroundType.valueOf(str.toUpperCase());
    } catch (IllegalArgumentException iae) {
        throw new GroundException("Invalid type: " + str + ".");
    }
  }

  @Override
  public String toString() {
    return this.name;
  }
}
