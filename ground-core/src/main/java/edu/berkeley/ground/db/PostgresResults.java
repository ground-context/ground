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

import edu.berkeley.ground.exceptions.GroundDBException;

import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;

public class PostgresResults implements QueryResults {
  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresResults.class);

  private ResultSet resultSet;

  public PostgresResults(ResultSet resultSet) {
    this.resultSet = resultSet;
  }

  public boolean next() throws GroundDBException {
    try {
      return this.resultSet.next();
    } catch (SQLException e) {
      throw new GroundDBException(e);
    }
  }

  public String getString(int index) throws GroundDBException {
    try {
      return resultSet.getString(index);
    } catch (SQLException e) {
      LOGGER.error(e.getMessage());

      throw new GroundDBException(e);
    }
  }

  public String getString(String field) {
    throw new NotImplementedException();
  }


  public long getLong(String field) {
    throw new NotImplementedException();
  }

  public int getInt(int index) throws GroundDBException {
    try {
      return resultSet.getInt(index);
    } catch (SQLException e) {
      LOGGER.error(e.getMessage());

      throw new GroundDBException(e);
    }
  }

  public boolean getBoolean(int index) throws GroundDBException {
    try {
      return resultSet.getBoolean(index);
    } catch (SQLException e) {
      LOGGER.error(e.getMessage());

      throw new GroundDBException(e);
    }
  }

  public long getLong(int index) throws GroundDBException {
    try {
      return resultSet.getLong(index);
    } catch (SQLException e) {
      LOGGER.error(e.getMessage());

      throw new GroundDBException(e);
    }
  }

  public boolean isNull(int index) throws GroundDBException {
    try {
      resultSet.getBlob(index);
      return resultSet.wasNull();
    } catch (SQLException e) {
      LOGGER.error(e.getMessage());

      throw new GroundDBException(e);
    }
  }

  public boolean isNull(String field) {
    throw new NotImplementedException();
  }
}
