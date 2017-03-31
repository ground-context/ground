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

package edu.berkeley.ground.model.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class EdgeVersion extends RichVersion {
  // the id of the Edge containing this Version
  private final long edgeId;

  // the first NodeVersion in fromNode that this EdgeVersion applies to
  private final long fromNodeVersionStartId;

  // the last NodeVersion in fromNode that this EdgeVersion applies to
  private final long fromNodeVersionEndId;

  // the first NodeVersion in toNode that this EdgeVersion applies to
  private final long toNodeVersionStartId;

  // the last NodeVersion in toNode that this EdgeVersion applies to
  private final long toNodeVersionEndId;

  /**
   * Create a new EdgeVersion.
   *
   * @param id the id of the edge version
   * @param tags the tags associated with the edge version
   * @param structureVersionId the id of the StructureVersion associated with this edge version
   * @param reference an optional external reference
   * @param referenceParameters the access parameters for the reference
   * @param edgeId the id of the edge containing this version
   * @param fromNodeVersionStartId the start node version id from this edge's from node
   * @param fromNodeVersionEndId the end node version id from this edge's from node
   * @param toNodeVersionStartId the start node version id from this edge's to node
   * @param toNodeVersionEndId the end node version id from this edge's to node
   */
  @JsonCreator
  public EdgeVersion(
      @JsonProperty("id") long id,
      @JsonProperty("tags") Map<String, Tag> tags,
      @JsonProperty("structureVersionId") long structureVersionId,
      @JsonProperty("reference") String reference,
      @JsonProperty("referenceParameters") Map<String, String> referenceParameters,
      @JsonProperty("edgeId") long edgeId,
      @JsonProperty("fromNodeVersionStartId") long fromNodeVersionStartId,
      @JsonProperty("fromNodeVersionEndId") long fromNodeVersionEndId,
      @JsonProperty("toNodeVersionStartId") long toNodeVersionStartId,
      @JsonProperty("toNodeVersionEndId") long toNodeVersionEndId) {

    super(id, tags, structureVersionId, reference, referenceParameters);

    this.edgeId = edgeId;

    this.fromNodeVersionStartId = fromNodeVersionStartId;
    this.fromNodeVersionEndId = fromNodeVersionEndId;
    this.toNodeVersionStartId = toNodeVersionStartId;
    this.toNodeVersionEndId = toNodeVersionEndId;
  }

  @JsonProperty
  public long getEdgeId() {
    return this.edgeId;
  }

  @JsonProperty
  public long getFromNodeVersionStartId() {
    return this.fromNodeVersionStartId;
  }

  @JsonProperty
  public long getFromNodeVersionEndId() {
    return this.fromNodeVersionEndId;
  }

  @JsonProperty
  public long getToNodeVersionStartId() {
    return this.toNodeVersionStartId;
  }

  @JsonProperty
  public long getToNodeVersionEndId() {
    return this.toNodeVersionEndId;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof EdgeVersion)) {
      return false;
    }

    EdgeVersion otherEdgeVersion = (EdgeVersion) other;

    return this.edgeId == otherEdgeVersion.edgeId
        && this.fromNodeVersionStartId == otherEdgeVersion.fromNodeVersionStartId
        && this.fromNodeVersionEndId == otherEdgeVersion.fromNodeVersionEndId
        && this.toNodeVersionStartId == otherEdgeVersion.toNodeVersionStartId
        && this.toNodeVersionEndId == otherEdgeVersion.toNodeVersionEndId
        && this.getId() == otherEdgeVersion.getId()
        && super.equals(other);
  }
}
