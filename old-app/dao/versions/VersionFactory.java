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

package dao.versions;

import exceptions.GroundException;
import exceptions.GroundUnsupportedOperationException;
import models.versions.Version;

public interface VersionFactory<T extends Version> {
  void insertIntoDatabase(long id) throws GroundException;

  Class<T> getType();

  T retrieveFromDatabase(long id) throws GroundException;

  default void deleteEdgeVersion() throws GroundException {
    throw new GroundUnsupportedOperationException(this.getType(), "deleteEdgeVersion");
  }

  default void deleteGraphVersion() throws GroundException {
    throw new GroundUnsupportedOperationException(this.getType(), "deleteGraphVersion");
  }

  default void deleteLineageEdgeVersion() throws GroundException {
    throw new GroundUnsupportedOperationException(this.getType(), "deleteLineageEdgeVersion");
  }

  default void deleteLineageGraphVersion() throws GroundException {
    throw new GroundUnsupportedOperationException(this.getType(), "deleteLineageGraphVersion");
  }

  default void deleteNodeVersion() throws GroundException {
    throw new GroundUnsupportedOperationException(this.getType(), "deleteNodeVersion");
  }

  default void deleteStructureVersion() throws GroundException {
    throw new GroundUnsupportedOperationException(this.getType(), "deleteStructureVersion");
  }
}
