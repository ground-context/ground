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

/**
 * An interface representing a specific row of a DbResults response.
 */
public interface DbRow {
  String getString(String field) throws GroundDbException;
  int getInt(String field) throws GroundDbException;
  boolean getBoolean(String field) throws GroundDbException;
  long getLong(String field) throws GroundDbException;
  boolean isNull(String field) throws GroundDbException;

  default Object getValue(GroundType type, String field)
      throws GroundDbException {

    if (type == null) {
      return null;
    }

    switch (type) {
      case STRING:
        return this.getString(field);
      case INTEGER:
        return this.getInt(field);
      case LONG:
        return this.getLong(field);
      case BOOLEAN:
        return this.getBoolean(field);
      default:
        // This should never happen because we've listed all types.
        throw new GroundDbException("Unidentified type: " + type);
    }
  }
}
