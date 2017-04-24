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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

/**
 * A wrapper for Postgres's result queries.
 *
 * WARNING: Due to how Java implements ResultSet, an instance of this
 * class can only be iterated through once. In addition, columns must be
 * queried in the same order they exist in the database.
 */
public class PostgresResults implements DbResults {
  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresResults.class);
  private final ResultSet resultSet;
  private final boolean isEmpty;

  public PostgresResults(ResultSet resultSet) throws GroundDbException {
    this.resultSet = resultSet;

    boolean isEmpty = true;
    try {
      // Move cursor to row 1.
      isEmpty = !this.resultSet.next();
    } catch (SQLException e) {
      throw new GroundDbException(e);
    }

    this.isEmpty = isEmpty;
  }

  @Override
  public Iterator<DbRow> iterator() {
    return new PostgresIterator(this.resultSet);
  }

  @Override
  public boolean isEmpty() {
    return this.isEmpty;
  }

  @Override
  public DbRow one() {
    try {
      return this.resultSet.isAfterLast() ? null : new PostgresRow(this.resultSet);
    } catch (SQLException e) {
      return null;
    }
  }

  @Override
  public void close() throws GroundDbException {
    try {
      this.resultSet.close();
    } catch (SQLException e) {
      throw new GroundDbException(e);
    }
  }

  private class PostgresIterator implements Iterator<DbRow> {
    private final ResultSet resultSet;
    private boolean didNext;
    private boolean hasNext;

    public PostgresIterator(ResultSet resultSet) {
      this.resultSet = resultSet;
      this.didNext = true;
      this.hasNext = !PostgresResults.this.isEmpty;
    }

    @Override
    public boolean hasNext() {
      if (!this.didNext) {
        try {
          this.hasNext = this.resultSet.next();
        } catch (SQLException e) {
          LOGGER.error(e.getMessage());
        }

        this.didNext = true;
      }

      return this.hasNext;
    }

    @Override
    public DbRow next() {
      if (!this.didNext) {
        try {
          this.resultSet.next();
        } catch (SQLException e) {
          LOGGER.error(e.getMessage());
        }
      }

      this.didNext = false;
      return new PostgresRow(this.resultSet);
    }
  }

  private class PostgresRow implements DbRow {
    private final ResultSet resultSet;

    public PostgresRow(ResultSet resultSet) {
      this.resultSet = resultSet;
    }

    /**
     * Retrieve the string at the index.
     *
     * @param field the column to look in
     * @return the string at index
     * @throws GroundDbException either the column doesn't exist or it isn't a string
     */
    @Override
    public String getString(String field) throws GroundDbException {
      try {
        return this.resultSet.getString(field);
      } catch (SQLException e) {
        throw new GroundDbException(e);
      }
    }

    /**
     * Retrieve the int at the index.
     *
     * @param field the column to look in
     * @return the int at index
     * @throws GroundDbException either column doesn't exist or isn't an int
     */
    @Override
    public int getInt(String field) throws GroundDbException {
      try {
        return this.resultSet.getInt(field);
      } catch (SQLException e) {
        throw new GroundDbException(e);
      }
    }

    /**
     * Retrieve the long at the index.
     *
     * @param field the column to look in
     * @return the long at the index
     * @throws GroundDbException either the column doesn't exist or isn't a long
     */
    @Override
    public long getLong(String field) throws GroundDbException {
      try {
        return this.resultSet.getLong(field);
      } catch (SQLException e) {
        throw new GroundDbException(e);
      }
    }


    /**
     * Retrieve the boolean at the index.
     *
     * @param field the index to use
     * @return the boolean at index
     * @throws GroundDbException either column doesn't exist or isn't an boolean
     */
    @Override
    public boolean getBoolean(String field) throws GroundDbException {
      try {
        return this.resultSet.getBoolean(field);
      } catch (SQLException e) {
        throw new GroundDbException(e);
      }
    }

    /**
     * Determine if the index of current row is null.
     *
     * @param field the column to look in
     * @return true if null, false otherwise
     */
    @Override
    public boolean isNull(String field) throws GroundDbException {
      try {
        this.resultSet.getBlob(field);
        return this.resultSet.wasNull();
      } catch (SQLException e) {
        throw new GroundDbException(e);
      }
    }
  }
}
