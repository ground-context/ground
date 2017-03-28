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

import edu.berkeley.ground.exceptions.GroundDbException;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PostgresResults implements QueryResults {
  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresResults.class);

  private final ResultSet resultSet;

  public PostgresResults(ResultSet resultSet) {
    this.resultSet = resultSet;
  }

  /**
   * Move on to the next column in the result set.
   *
   * @return false if there are no more rows
   */
  @Override
  public boolean next() throws GroundDbException {
    try {
      return this.resultSet.next();
    } catch (SQLException e) {
      throw new GroundDbException(e);
    }
  }

  /**
   * Retrieve the string at the index.
   *
   * @param index the index to use
   * @return the string at index
   * @throws GroundDbException either the column doesn't exist or it isn't a string
   */
  @Override
  public String getString(int index) throws GroundDbException {
    try {
      return resultSet.getString(index);
    } catch (SQLException e) {
      LOGGER.error(e.getMessage());

      throw new GroundDbException(e);
    }
  }

  /**
   * Return the string in the column with the given name.
   *
   * @param field the column to look in
   * @return the string in the column
   */
  @Override
  public String getString(String field) {
    throw new NotImplementedException();
  }

  /**
   * Retrieve the int at the index.
   *
   * @param index the index to use
   * @return the int at index
   * @throws GroundDbException either column doesn't exist or isn't an int
   */
  @Override
  public int getInt(int index) throws GroundDbException {
    try {
      return resultSet.getInt(index);
    } catch (SQLException e) {
      LOGGER.error(e.getMessage());

      throw new GroundDbException(e);
    }
  }

  /**
   * Retrieve the long in the column with name field.
   *
   * @param field the name of the column
   * @return the long in field
   */
  @Override
  public long getLong(String field) {
    throw new NotImplementedException();
  }

  /**
   * Retrieve the long at the index.
   *
   * @param index the index to use
   * @return the long at the index
   * @throws GroundDbException either the column doesn't exist or isn't a long
   */
  @Override
  public long getLong(int index) throws GroundDbException {
    try {
      return resultSet.getLong(index);
    } catch (SQLException e) {
      LOGGER.error(e.getMessage());

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
  @Override
  public boolean getBoolean(int index) throws GroundDbException {
    try {
      return resultSet.getBoolean(index);
    } catch (SQLException e) {
      LOGGER.error(e.getMessage());

      throw new GroundDbException(e);
    }
  }

  /**
   * Determine if the index of current row is null.
   *
   * @param index the index to use
   * @return true if null, false otherwise
   */
  @Override
  public boolean isNull(int index) throws GroundDbException {
    try {
      resultSet.getBlob(index);
      return resultSet.wasNull();
    } catch (SQLException e) {
      LOGGER.error(e.getMessage());

      throw new GroundDbException(e);
    }
  }

  /**
   * Determine if the index of current row is null.
   *
   * @param field the field to check
   * @return true if null, false otherwise
   */
  @Override
  public boolean isNull(String field) {
    throw new NotImplementedException();
  }
}
