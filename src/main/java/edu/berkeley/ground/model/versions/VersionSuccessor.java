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

package edu.berkeley.ground.model.versions;

import com.fasterxml.jackson.annotation.JsonProperty;


public class VersionSuccessor<T extends Version> {

  // the unique id of this VersionSuccessor
  private final long id;

  // the id of the Version that originates this successor
  private final long fromId;

  // the id of the Version that this success points to
  private final long toId;

  /**
   * Create a version successor.
   *
   * @param id the id of the successor
   * @param fromId the source id
   * @param toId the destination id
   */
  public VersionSuccessor(long id, long fromId, long toId) {
    this.id = id;
    this.fromId = fromId;
    this.toId = toId;
  }

  @JsonProperty
  public long getId() {
    return this.id;
  }

  @JsonProperty
  public long getFromId() {
    return this.fromId;
  }

  @JsonProperty
  public long getToId() {
    return this.toId;
  }
}
