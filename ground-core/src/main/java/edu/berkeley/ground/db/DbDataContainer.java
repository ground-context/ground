/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.db;

import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

public class DbDataContainer {
  // the name of the field
  private String field;

  // the type of the field
  private GroundType groundType;

  // the value of the field;
  private Object value;

  public DbDataContainer(String field, GroundType groundType, Object value) throws GroundException {
    if (value != null && !(value.getClass().equals(groundType.getTypeClass()))) {
      throw new GroundException("Value of type " + value.getClass().toString() + " does not correspond to type of " + groundType.getTypeClass().toString() + ".");
    }

    this.field = field;
    this.groundType = groundType;
    this.value = value;
  }

  public String getField() {
    return this.field;
  }

  public GroundType getGroundType() {
    return this.groundType;
  }

  public Object getValue() {
    return this.value;
  }
}
