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

import edu.berkeley.ground.exceptions.GroundDBException;

public class CassandraResults implements QueryResults {
  private ResultSet resultSet;
  private Row currentRow;

  public CassandraResults(ResultSet resultSet) {
    this.resultSet = resultSet;
    this.currentRow = null;
  }

  public String getString(int index) throws GroundDBException {
    try {
      return this.currentRow.getString(index);
    } catch (Exception e) {
      throw new GroundDBException(e);
    }
  }

  public String getString(String field) throws GroundDBException {
    try {
      return this.currentRow.getString(field);
    } catch (Exception e) {
      throw new GroundDBException(e);
    }
  }

  public int getInt(int index) throws GroundDBException {
    try {
      return this.currentRow.getInt(index);
    } catch (Exception e) {
      throw new GroundDBException(e);
    }
  }

  public boolean getBoolean(int index) throws GroundDBException {
    try {
      return this.currentRow.getBool(index);
    } catch (Exception e) {
      throw new GroundDBException(e);
    }
  }

  public long getLong(int index) throws GroundDBException {
    try {
      return this.currentRow.getLong(index);
    } catch (Exception e) {
      throw new GroundDBException(e);
    }
  }

  public long getLong(String field) throws GroundDBException {
    try {
      return this.currentRow.getLong(field);
    } catch (Exception e) {
      throw new GroundDBException(e);
    }
  }

  public boolean next() {
    this.currentRow = this.resultSet.one();

    return this.currentRow != null;
  }

  public boolean isNull(int index) {
    return this.currentRow.isNull(index);
  }

  public boolean isNull(String field) {
    return this.currentRow.isNull(field);
  }
}
