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

import edu.berkeley.ground.exceptions.GroundException;

import java.util.List;

public interface QueryResults {
  String getString(int index) throws GroundException;

  String getString(String field) throws GroundException;

  int getInt(int index) throws GroundException;

  boolean getBoolean(int index) throws GroundException;

  long getLong(int index) throws GroundException;

  long getLong(String field) throws GroundException;

  List<String> getStringList(int index) throws GroundException;

  boolean next() throws GroundException;
}