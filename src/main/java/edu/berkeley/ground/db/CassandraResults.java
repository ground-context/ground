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

package edu.berkeley.ground.db;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import edu.berkeley.ground.exceptions.GroundDbException;

public class CassandraResults implements QueryResults {
  private final ResultSet resultSet;
  private Row currentRow;

  public CassandraResults(ResultSet resultSet) {
    this.resultSet = resultSet;
    this.currentRow = null;
    this.next();
  }

  /**
   * Retrieve the string at the index.
   *
   * @param index the index to use
   * @return the string at index
   * @throws GroundDbException either the column doesn't exist or it isn't a string
   */
  public String getString(int index) throws GroundDbException {
    try {
      return this.currentRow.getString(index);
    } catch (Exception e) {
      throw new GroundDbException(e);
    }
  }

  /**
   * Return the string in the column with the given name.
   *
   * @param field the column to look in
   * @return the string in the column
   * @throws GroundDbException either the column doesn't exist or it isn't a string
   */
  public String getString(String field) throws GroundDbException {
    try {
      return this.currentRow.getString(field);
    } catch (Exception e) {
      throw new GroundDbException(e);
    }
  }

  /**
   * Retrieve the int at the index.
   *
   * @param index the index to use
   * @return the int at index
   * @throws GroundDbException either column doesn't exist or isn't an int
   */
  public int getInt(int index) throws GroundDbException {
    try {
      return this.currentRow.getInt(index);
    } catch (Exception e) {
      throw new GroundDbException(e);
    }
  }

  /**
   * Retrieve the boolean at the index.
   *
   * @param index the index to use
   * @return the boolean at index
   * @throws GroundDbException either column doesn't exist or isn't an boolean
   */
  public boolean getBoolean(int index) throws GroundDbException {
    try {
      return this.currentRow.getBool(index);
    } catch (Exception e) {
      throw new GroundDbException(e);
    }
  }

  /**
   * Retrieve the long at the index.
   *
   * @param index the index to use
   * @return the long at the index
   * @throws GroundDbException either the column doesn't exist or isn't a long
   */
  public long getLong(int index) throws GroundDbException {
    try {
      return this.currentRow.getLong(index);
    } catch (Exception e) {
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
  public long getLong(String field) throws GroundDbException {
    try {
      return this.currentRow.getLong(field);
    } catch (Exception e) {
      throw new GroundDbException(e);
    }
  }

  /**
   * Move on to the next column in the result set.
   *
   * @return false if there are no more rows
   */
  public boolean next() {
    this.currentRow = this.resultSet.one();

    return this.currentRow != null;
  }

  /**
   * Determine if the index of current row is null.
   *
   * @param index the index to use
   * @return true if null, false otherwise
   */
  public boolean isNull(int index) {
    return this.currentRow.isNull(index);
  }

  /**
   * Determine if the index of current row is null.
   *
   * @param field the field to check
   * @return true if null, false otherwise
   */
  public boolean isNull(String field) {
    return this.currentRow.isNull(field);
  }
}
