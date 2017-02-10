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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class GraphVersion extends RichVersion {
  // the id of the Graph that contains this Version
  private long graphId;

  // the list of ids of EdgeVersions in this GraphVersion
  private List<Long> edgeVersionIds;

  @JsonCreator
  public GraphVersion(@JsonProperty("id") long id,
                      @JsonProperty("tags") Map<String, Tag> tags,
                      @JsonProperty("structureVersionId") long structureVersionId,
                      @JsonProperty("reference") String reference,
                      @JsonProperty("referenceParameters") Map<String, String> referenceParameters,
                      @JsonProperty("graphId") long graphId,
                      @JsonProperty("edgeVersionIds") List<Long> edgeVersionIds) {

    super(id, tags, structureVersionId, reference, referenceParameters);

    this.graphId = graphId;
    this.edgeVersionIds = edgeVersionIds;
  }

  @JsonProperty
  public long getGraphId() {
    return this.graphId;
  }

  @JsonProperty
  public List<Long> getEdgeVersionIds() {
    return this.edgeVersionIds;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof GraphVersion)) {
      return false;
    }

    GraphVersion otherGraphVersion = (GraphVersion) other;

    return this.graphId == otherGraphVersion.graphId &&
        this.edgeVersionIds.equals(otherGraphVersion.edgeVersionIds) &&
        super.equals(other);
  }
}
