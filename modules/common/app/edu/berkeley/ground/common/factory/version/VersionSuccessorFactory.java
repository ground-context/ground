/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.ground.common.factory.version;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.version.Version;
import edu.berkeley.ground.common.model.version.VersionSuccessor;

import java.util.List;

public interface VersionSuccessorFactory {
  <T extends Version> VersionSuccessor<T> create(long fromId, long toId) throws GroundException;

  <T extends Version> VersionSuccessor<T> retrieveFromDatabase(long dbId) throws GroundException;

  void deleteFromDestination(List<String> sqlList, long toId, long itemId) throws GroundException;
}
