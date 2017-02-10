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
import edu.berkeley.ground.exceptions.GroundException;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface DBClient {
  List<String> SELECT_STAR = Stream.of("*").collect(Collectors.toList());

  GroundDBConnection getConnection() throws GroundDBException;

  abstract class GroundDBConnection {
    public abstract void commit() throws GroundDBException;

    public abstract void abort() throws GroundDBException;

    /**
     * Run transitive closure from nodeVersionId.
     *
     * @param nodeVersionId the start id
     * @return the list of reachable ids
     */
    public abstract List<Long> transitiveClosure(long nodeVersionId) throws GroundException;
  }
}
