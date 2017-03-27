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

import edu.berkeley.ground.exceptions.GroundDbException;

public interface QueryResults {
  String getString(int index) throws GroundDbException;

  String getString(String field) throws GroundDbException;

  int getInt(int index) throws GroundDbException;

  boolean getBoolean(int index) throws GroundDbException;

  long getLong(int index) throws GroundDbException;

  long getLong(String field) throws GroundDbException;

  boolean next() throws GroundDbException;

  boolean isNull(int index) throws GroundDbException;

  boolean isNull(String name) throws GroundDbException;
}
