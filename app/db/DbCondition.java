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

package db;

import exceptions.GroundDbException;
import models.versions.GroundType;

import java.util.List;

public abstract class DbCondition<T> {
  // The name of the field.
  protected final String field;

  // The type of the field.
  protected final GroundType groundType;

  public DbCondition(String field, GroundType groundType) {
    this.field = field;
    this.groundType = groundType;
  }

  public String getField() {
    return this.field;
  }

  public GroundType getGroundType() {
    return this.groundType;
  }

  abstract public String getPredicate();

  abstract public List<T> getValues();

  protected void verifyType(Object value) throws GroundDbException {
    if (value != null && !(value.getClass().equals(groundType.getTypeClass()))) {
      throw new GroundDbException("Value of type " + value.getClass().toString()
        + " does not correspond to type of " + groundType.getTypeClass().toString() + ".");
    }
  }
}
