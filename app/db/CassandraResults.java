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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import com.datastax.driver.core.exceptions.CodecNotFoundException;
import exceptions.GroundDbException;
import models.versions.GroundType;

public class CassandraResults implements DbResults {
  private final ResultSet resultSet;
  private Row currentRow;

  public CassandraResults(ResultSet resultSet) {
    this.resultSet = resultSet;
    this.currentRow = null;
    this.next();
  }

  /**
   * Return the string in the column with the given name.
   *
   * @param field the column to look in
   * @return the string in the column
   * @throws GroundDbException either the column doesn't exist or it isn't a string
   */
  @Override
  public String getString(String field) throws GroundDbException {
    try {
      return this.currentRow.getString(field);
    } catch (IllegalArgumentException | CodecNotFoundException e) {
      throw new GroundDbException(e);
    }
  }

  /**
   * Retrieve the int in the column with the given name.
   *
   * @param field the column to look in
   * @return the int in the column
   * @throws GroundDbException either column doesn't exist or isn't an int
   */
  @Override
  public int getInt(String field) throws GroundDbException {
    try {
      return (Integer) GroundType.INTEGER.parse(this.currentRow.getString(field));
    } catch (IllegalArgumentException | CodecNotFoundException e) {
      throw new GroundDbException(e);
    }
  }

  /**
   * Retrieve the boolean in the column with the given name.
   *
   * @param field the column to look in
   * @return the boolean in the column
   * @throws GroundDbException either column doesn't exist or isn't an boolean
   */
  @Override
  public boolean getBoolean(String field) throws GroundDbException {
    try {
      return (Boolean) GroundType.BOOLEAN.parse(this.currentRow.getString(field));
    } catch (IllegalArgumentException | CodecNotFoundException e) {
      throw new GroundDbException(e);
    }
  }

  /**
   * Retrieve the long in the column with name field.
   *
   * @param field the name of the column
   * @return the long in field
   * @throws GroundDbException either column doesn't exist or isn't a long
   */
  @Override
  public long getLong(String field) throws GroundDbException {
    try {
      //Q: Why Long one does not follow the same logic as the other types?
      return this.currentRow.getLong(field);
    } catch (IllegalArgumentException | CodecNotFoundException e) {
      throw new GroundDbException(e);
    }
  }

  /**
   * Move on to the next column in the result set.
   *
   * @return false if there are no more rows
   */
  @Override
  public boolean next() {
    this.currentRow = this.resultSet.one();

    return this.currentRow != null;
  }

  /**
   * Determine if the index of current row is null.
   *
   * @param field the field to check
   * @return true if null, false otherwise
   */
  @Override
  public boolean isNull(String field) {
    return this.currentRow.isNull(field);
  }

  @Override
  public boolean isEmpty() {
    return (this.resultSet == null || this.resultSet.isExhausted()) && this.currentRow == null;
  }
}
