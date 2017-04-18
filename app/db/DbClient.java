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

import java.util.Collections;
import java.util.List;

public interface DbClient extends AutoCloseable {
  List<String> SELECT_STAR = Collections.singletonList("*");

  void commit() throws GroundDbException;
  void abort() throws GroundDbException;

  DbResults equalitySelect(String table, List<String> projection,
                           List<DbDataContainer> predicatesAndValues) throws GroundDbException;
  void insert(String table, List<DbDataContainer> insertValues) throws GroundDbException;
  void delete(List<DbDataContainer> predicates, String table) throws GroundDbException;
}
