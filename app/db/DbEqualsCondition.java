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

import java.util.Arrays;
import java.util.List;

public class DbEqualsCondition<T> extends DbCondition<T> {
  // The value of the field.
  private final T value;

  /**
   * Create an "equals" condition for use in WHERE clauses.
   *
   * @param field the name of the field
   * @param groundType the type of value
   * @param value the value of the field
   * @throws GroundDbException mismatch between groundType and value's type
   */
  public DbEqualsCondition(String field, GroundType groundType, T value)
      throws GroundDbException {
    super(field, groundType);

    verifyType(value);
    this.value = value;
  }

  public T getValue() {
    return this.value;
  }

  @Override
  public String getPredicate() {
    return this.field + " = ?";
  }

  @Override
  public List<T> getValues() {
    return Arrays.asList(value);
  }
}
