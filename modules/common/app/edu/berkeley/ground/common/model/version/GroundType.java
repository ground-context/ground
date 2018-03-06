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
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.exception.GroundException.ExceptionType;
import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Arrays;

public enum GroundType {
  STRING(String.class, "string", Types.VARCHAR) {
    @Override
    public Object parse(String str) {
      return str;
    }
  },
  INTEGER(Integer.class, "integer", Types.INTEGER) {
    @Override
    public Object parse(String str) {
      return Integer.parseInt(str);
    }
  },
  BOOLEAN(Boolean.class, "boolean", Types.BOOLEAN) {
    @Override
    public Object parse(String str) {
      return Boolean.parseBoolean(str);
    }
  },
  LONG(Long.class, "long", Types.BIGINT) {
    @Override
    public Object parse(String str) {
      return Long.parseLong(str);
    }
  },
  DOUBLE(Double.class, "double", Types.DOUBLE) {
    @Override
    public Object parse(String str) {
      return Double.parseDouble(str);
      //throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }
  },
  NULL(null, "null", Types.NULL) {
    @Override
    public Object parse(String str) {
      return null;
      //throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }
  },
  ARRAY(ArrayList.class, "array", Types.ARRAY) {
    @Override
    public Object parse(String str) {
      //return new ArrayList<String>(Arrays.asList(str.split("\\s*,\\s*")));
      //return str;
      throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }
  },
  JSON(LinkedHashMap.class, "json", Types.VARCHAR) {
    @Override
    public Object parse(String str) {
      /*
      LinkedHashMap<String, String> myMap = new LinkedHashMap<String, String>();
      String[] pairs = str.split(",");
      for (int i=0;i<pairs.length;i++) {
        String pair = pairs[i];
        String[] keyValue = pair.split(":");
        myMap.put(keyValue[0], keyValue[1]);
        
      }
      return myMap;
      */
      //return str;
      throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }
  },
  SET(ArrayList.class, "set", Types.ARRAY){
    @Override
    public Object parse(String str) {
      //return str;
      throw new java.lang.UnsupportedOperationException("Not supported yet.");
    }
  //},
  //SET(String.class, "set", Types.VARCHAR){
  //  @Override
  //  public Object parse(String str) {
  //    //return str;
  //    throw new java.lang.UnsupportedOperationException("Not supported yet.");
  //  }
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
   *
   * @return an integer the corresponding SQL Type
   */
  public int getSqlType() {
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
      throw new GroundException(ExceptionType.OTHER, String.format("Invalid type: %s.", str));
    }
  }

  @Override
  public String toString() {
    return this.name;
  }
}
