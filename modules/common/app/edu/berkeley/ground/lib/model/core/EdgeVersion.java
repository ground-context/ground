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
package edu.berkeley.ground.lib.model.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.berkeley.ground.lib.model.version.Tag;
import java.util.List;
import java.util.Map;

public class EdgeVersion extends RichVersion {
  // the id of the Edge containing this Version
  @JsonProperty("edge_id")
  private final long edgeId;

  // the first NodeVersion in fromNode that this EdgeVersion applies to
  @JsonProperty("from_node_version_start_id")
  private final long fromNodeVersionStartId;

  // the last NodeVersion in fromNode that this EdgeVersion applies to
  @JsonProperty("from_node_version_end_id")
  private final long fromNodeVersionEndId;

  // the first NodeVersion in toNode that this EdgeVersion applies to
   @JsonProperty("to_node_version_start_id")
  private final long toNodeVersionStartId;

  // the last NodeVersion in toNode that this EdgeVersion applies to
   @JsonProperty("to_node_version_end_id")
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
      @JsonProperty("structure_version_id") long structureVersionId,
      @JsonProperty("reference") String reference,
      @JsonProperty("reference_parameters") Map<String, String> referenceParameters,
      @JsonProperty("edge_id") long edgeId,
      @JsonProperty("from_node_version_start_id") long fromNodeVersionStartId,
      @JsonProperty("from_node_version_end_id") long fromNodeVersionEndId,
      @JsonProperty("to_node_version_start_id") long toNodeVersionStartId,
      @JsonProperty("to_node_version_end_id") long toNodeVersionEndId) {

    super(id, tags, structureVersionId, reference, referenceParameters);

    this.edgeId = edgeId;

    this.fromNodeVersionStartId = fromNodeVersionStartId;
    this.fromNodeVersionEndId = fromNodeVersionEndId;
    this.toNodeVersionStartId = toNodeVersionStartId;
    this.toNodeVersionEndId = toNodeVersionEndId;
  }

  public long getEdgeId() {
    return this.edgeId;
  }

  public long getFromNodeVersionStartId() {
    return this.fromNodeVersionStartId;
  }

  public long getFromNodeVersionEndId() {
    return this.fromNodeVersionEndId;
  }

  public long getToNodeVersionStartId() {
    return this.toNodeVersionStartId;
  }

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
