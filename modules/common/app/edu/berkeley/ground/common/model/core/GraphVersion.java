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
package edu.berkeley.ground.common.model.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.berkeley.ground.common.model.version.Tag;
import java.util.List;
import java.util.Map;

public class GraphVersion extends RichVersion {
  @JsonProperty("tags")
  private final Map<String, Tag> tags;
  @JsonProperty("structureVersionId")
  private final long structureVersionId;
  @JsonProperty("reference")
  private final String reference;
  @JsonProperty("referenceParameters")
  private final Map<String, String> referenceParameters;
  @JsonProperty("graphId")
  private final long graphId;
  @JsonProperty("edgeVersionIds")
  private final List<Long> edgeVersionIds;

  /**
   * Create a new graph version.
   *
   * @param id the id of this graph version
   * @param tags the tags associated with this graph version
   * @param structureVersionId the id of the StructureVersion associated with this graph version
   * @param reference an optional external reference
   * @param referenceParameters the access parameters of the reference
   * @param graphId the id of the graph containing this version
   * @param edgeVersionIds the list of edge versions in this graph version
   */
  @JsonCreator
  public GraphVersion(
      @JsonProperty("id") long id,
      @JsonProperty("tags") Map<String, Tag> tags,
      @JsonProperty("structureVersionId") long structureVersionId,
      @JsonProperty("reference") String reference,
      @JsonProperty("referenceParameters") Map<String, String> referenceParameters,
      @JsonProperty("graphId") long graphId,
      @JsonProperty("edgeVersionIds") List<Long> edgeVersionIds) {

    super(id, tags, structureVersionId, reference, referenceParameters);

    this.tags = tags;
    this.structureVersionId = structureVersionId;
    this.reference = reference;
    this.referenceParameters = referenceParameters;
    this.graphId = graphId;
    this.edgeVersionIds = edgeVersionIds;
  }

  public Map<String, Tag> getTags() {
    return this.tags;
  }

  public Long getStructureVersionId() {
    return this.structureVersionId;
  }

  public String getReference() {
    return this.reference;
  }

  public Map<String, String> getReferenceParameters() {
    return this.referenceParameters;
  }

  public long getGraphId() {
    return this.graphId;
  }

  public List<Long> getEdgeVersionIds() {
    return this.edgeVersionIds;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof GraphVersion)) {
      return false;
    }

    GraphVersion otherGraphVersion = (GraphVersion) other;

    return this.graphId == otherGraphVersion.graphId
        && this.edgeVersionIds.equals(otherGraphVersion.edgeVersionIds)
        && super.equals(other);
  }
}
