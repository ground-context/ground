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
import edu.berkeley.ground.common.model.version.Item;
import edu.berkeley.ground.common.model.version.Tag;
import java.util.Map;

public class Graph extends Item {

  // the name of this Graph
  @JsonProperty("name")
  private final String name;

  // the source key for this Graph
  @JsonProperty("sourceKey")
  private final String sourceKey;

  /**
   * Create a new Graph.
   *
   * @param id the id of the graph
   * @param name the name of the graph
   * @param sourceKey the user-generated unique key for the graph
   * @param tags the tags associated with the graph
   */
  @JsonCreator
  public Graph(@JsonProperty("itemId") long id,
                @JsonProperty("name") String name,
                @JsonProperty("sourceKey") String sourceKey,
                @JsonProperty("tags") Map<String, Tag> tags) {
    super(id, tags);

    this.name = name;
    this.sourceKey = sourceKey;
  }

  public Graph(long id, Graph other) {
    super(id, other.getTags());

    this.name = other.name;
    this.sourceKey = other.sourceKey;
  }

  public String getName() {
    return this.name;
  }

  public String getSourceKey() {
    return this.sourceKey;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Graph)) {
      return false;
    }

    Graph otherGraph = (Graph) other;

    return this.name.equals(otherGraph.name)
             && this.getId() == otherGraph.getId()
             && this.sourceKey.equals(otherGraph.sourceKey)
             && this.getTags().equals(otherGraph.getTags());
  }
}
