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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.Item;

import java.util.Map;

public class LineageGraph extends Item<LineageGraphVersion> {
  // the name of this graph
  private final String name;

  // the source key for this Node
  private final String sourceKey;

  /**
   * Create a new lineage graph.
   *
   * @param id the id of lineage graph
   * @param name the name of the lineage graph
   * @param sourceKey the user-generated unique key for the graph
   * @param tags the tags associated with the graph
   */
  @JsonCreator
  public LineageGraph(@JsonProperty("id") long id,
                      @JsonProperty("name") String name,
                      @JsonProperty("sourceKey") String sourceKey,
                      @JsonProperty("tags") Map<String, Tag> tags) {
    super(id, tags);

    this.name = name;
    this.sourceKey = sourceKey;
  }

  @JsonProperty
  public String getName() {
    return this.name;
  }

  @JsonProperty
  public String getSourceKey() {
    return this.sourceKey;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof LineageGraph)) {
      return false;
    }

    LineageGraph otherLineageGraph = (LineageGraph) other;

    return this.name.equals(otherLineageGraph.name)
        && this.getId() == otherLineageGraph.getId()
        && this.sourceKey.equals(otherLineageGraph.sourceKey)
        && this.getTags().equals(otherLineageGraph.getTags());
  }
}
