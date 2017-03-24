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

package edu.berkeley.ground.model.usage;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.codehaus.jackson.annotate.JsonCreator;

import java.util.List;
import java.util.Map;

import edu.berkeley.ground.model.models.RichVersion;
import edu.berkeley.ground.model.models.Tag;

public class LineageGraphVersion extends RichVersion {
  // the id of the LineageGraph that contains this version
  private long lineageGraphId;

  // the list of ids of LineageEdgeVersions that are in this LineageGraph
  private List<Long> lineageEdgeVersionIds;

  @JsonCreator
  public LineageGraphVersion(@JsonProperty("id") long id,
                             @JsonProperty("tags") Map<String, Tag> tags,
                             @JsonProperty("structureVersionId") long structureVersionId,
                             @JsonProperty("reference") String reference,
                             @JsonProperty("referenceParameters") Map<String, String> referenceParameters,
                             @JsonProperty("graphId") long lineageGraphId,
                             @JsonProperty("edgeVersionIds") List<Long> lineageEdgeVersionIds) {

    super(id, tags, structureVersionId, reference, referenceParameters);

    this.lineageGraphId = lineageGraphId;
    this.lineageEdgeVersionIds = lineageEdgeVersionIds;
  }

  @JsonProperty
  public long getLineageGraphId() {
    return  this.lineageGraphId;
  }

  @JsonProperty
  public List<Long> getLineageEdgeVersionIds() {
    return this.lineageEdgeVersionIds;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof LineageGraphVersion)) {
      return false;
    }

    LineageGraphVersion otherLineageGraphVersion = (LineageGraphVersion) other;

    return this.lineageGraphId == otherLineageGraphVersion.lineageGraphId &&
        this.lineageEdgeVersionIds.equals(otherLineageGraphVersion.lineageEdgeVersionIds) &&
        super.equals(other);
  }
}
