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

import java.util.Iterator;

public class CassandraResults implements DbResults {
  private final ResultSet resultSet;
  private final boolean isEmpty;

  public CassandraResults(ResultSet resultSet) {
    this.resultSet = resultSet;
    this.isEmpty = this.resultSet.isExhausted();
  }

  @Override
  public Iterator<DbRow> iterator() {
    return new CassandraIterator(this.resultSet.iterator());
  }

  @Override
  public boolean isEmpty() {
    return this.isEmpty;
  }

  @Override
  public DbRow one() {
    return new CassandraRow(this.resultSet.one());
  }

  @Override
  public void close() {}

  private class CassandraIterator implements Iterator<DbRow> {
    private final Iterator<Row> iter;

    public CassandraIterator(Iterator<Row> iter) {
      this.iter = iter;
    }

    @Override
    public boolean hasNext() {
      return this.iter.hasNext();
    }

    @Override
    public DbRow next() {
      return new CassandraRow(this.iter.next());
    }
  }

  private class CassandraRow implements DbRow {
    private final Row row;

    public CassandraRow(Row row) {
      this.row = row;
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
        return this.row.getString(field);
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
        return (int) GroundType.INTEGER.parse(this.row.getString(field));
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
        return (boolean) GroundType.BOOLEAN.parse(this.row.getString(field));
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
        return this.row.getLong(field);
      } catch (IllegalArgumentException | CodecNotFoundException e) {
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
      return this.row.isNull(field);
    }
  }
}
