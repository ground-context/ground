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

public class DbBetweenCondition<T> extends DbCondition<T> {
  // The beginning value of the field, inclusive.
  private final T fromValue;

  // The ending value of the field, inclusive.
  private final T toValue;

  /**
   * Create an inclusive "between" condition for use in WHERE clauses.
   *
   * @param field the name of the field
   * @param groundType the type of value
   * @param fromValue the beginning value of the field
   * @throws GroundDbException mismatch between groundType and value's type
   */
  public DbBetweenCondition(String field, GroundType groundType, T fromValue, T toValue)
      throws GroundDbException {
    super(field, groundType);

    verifyType(fromValue);
    verifyType(toValue);

    this.fromValue = fromValue;
    this.toValue = toValue;
  }

  @Override
  public String getPredicate() {
    return String.format("%s >= ? AND %s <= ?", this.field, this.field);
  }

  @Override
  public List<T> getValues() {
    return Arrays.asList(fromValue, toValue);
  }
}
