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

package edu.berkeley.ground.api.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class EdgeVersion extends RichVersion {
  // the id of the Edge containing this Version
  private long edgeId;

  // the id of the NodeVersion that this EdgeVersion originates from
  private long fromId;

  // the id of the NodeVersion that this EdgeVersion points to
  private long toId;

  @JsonCreator
  public EdgeVersion(
      @JsonProperty("id") long id,
      @JsonProperty("tags") Map<String, Tag> tags,
      @JsonProperty("structureVersionId") long structureVersionId,
      @JsonProperty("reference") String reference,
      @JsonProperty("referenceParameters") Map<String, String> referenceParameters,
      @JsonProperty("edgeId") long edgeId,
      @JsonProperty("fromId") long fromId,
      @JsonProperty("toId") long toId) {

    super(id, tags, structureVersionId, reference, referenceParameters);

    this.edgeId = edgeId;
    this.fromId = fromId;
    this.toId = toId;
  }

  @JsonProperty
  public long getEdgeId() {
    return this.edgeId;
  }

  @JsonProperty
  public long getFromId() {
    return this.fromId;
  }

  @JsonProperty
  public long getToId() {
    return this.toId;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof EdgeVersion)) {
      return false;
    }

    EdgeVersion otherEdgeVersion = (EdgeVersion) other;

    return this.edgeId == otherEdgeVersion.edgeId &&
        this.fromId == otherEdgeVersion.fromId &&
        this.toId == otherEdgeVersion.toId &&
        this.getId() == otherEdgeVersion.getId() &&
        super.equals(other);
  }
}
