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

package edu.berkeley.ground.postgres.utils;

import edu.berkeley.ground.lib.exception.GroundException;

import java.util.Collections;
import java.util.List;

public abstract class DbClient implements AutoCloseable {
  public static final List<String> SELECT_STAR = Collections.singletonList("*");

  public abstract void commit() throws GroundException;

  public abstract void abort() throws GroundException;
}