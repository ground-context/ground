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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import edu.berkeley.ground.exceptions.GroundException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CassandraResults implements QueryResults {
  private ResultSet resultSet;
  private Row currentRow;

  public CassandraResults(ResultSet resultSet) {
    this.resultSet = resultSet;
    this.currentRow = null;
  }

  public String getString(int index) throws GroundException {
    try {
      return this.currentRow.getString(index);
    } catch (Exception e) {
      throw new GroundException(e);
    }
  }

  public String getString(String field) throws GroundException {
    try {
      return this.currentRow.getString(field);
    } catch (Exception e) {
      throw new GroundException(e);
    }
  }

  public int getInt(int index) throws GroundException {
    try {
      return this.currentRow.getInt(index);
    } catch (Exception e) {
      throw new GroundException(e);
    }
  }

  public boolean getBoolean(int index) throws GroundException {
    try {
      return this.currentRow.getBool(index);
    } catch (Exception e) {
      throw new GroundException(e);
    }
  }

  public long getLong(int index) throws GroundException {
    try {
      return this.currentRow.getLong(index);
    } catch (Exception e) {
      throw new GroundException(e);
    }
  }

  public long getLong(String field) throws GroundException {
    try {
      return this.currentRow.getLong(field);
    } catch (Exception e) {
      throw new GroundException(e);
    }
  }

  public List<String> getStringList(int index) throws GroundException {
    try {
      Iterator<Row> rowIterator = this.resultSet.iterator();
      List<String> result = new ArrayList<>();

      while (rowIterator.hasNext()) {
        result.add(rowIterator.next().getString(index));
      }

      return result;
    } catch (Exception e) {
      throw new GroundException(e);
    }
  }

  public boolean next() throws GroundException {
    this.currentRow = this.resultSet.one();

    return this.currentRow != null;
  }
}
