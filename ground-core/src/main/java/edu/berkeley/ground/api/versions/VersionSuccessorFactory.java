/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.api.versions;

import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.exceptions.GroundException;

public abstract class VersionSuccessorFactory {
    public abstract <T extends Version> VersionSuccessor<T> create(GroundDBConnection connection, String fromId, String toId) throws GroundException;

    public abstract <T extends Version> VersionSuccessor<T> retrieveFromDatabase(GroundDBConnection connection, String dbId) throws GroundException;

    protected static <T extends Version> VersionSuccessor<T> construct(String id, String fromId, String toId) {
        return new VersionSuccessor<>(id, fromId, toId);
    }
}
