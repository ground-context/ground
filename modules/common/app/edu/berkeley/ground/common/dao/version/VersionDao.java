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
package edu.berkeley.ground.common.dao.version;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.version.Version;
import edu.berkeley.ground.common.util.DbStatements;

public interface VersionDao<T extends Version> {

  DbStatements insert(T version) throws GroundException;

  Version retrieveFromDatabase(long id) throws GroundException;

  Class<T> getType();

  DbStatements delete(long id);
}
