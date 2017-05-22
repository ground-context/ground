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

public class Edge extends Item {

  // the name of this Edge
  @JsonProperty("name")
  private final String name;

  // the id of the Node that this EdgeVersion originates from
  @JsonProperty("fromNodeId")
  private final long fromNodeId;

  // the id of the Node that this EdgeVersion points to
  @JsonProperty("toNodeId")
  private final long toNodeId;

  // the source key for this Edge
  @JsonProperty("sourceKey")
  private final String sourceKey;

  /**
   * Construct a new Edge.
   *
   * @param id the edge id
   * @param name the edge name
   * @param sourceKey the user-generated unique key for the edge
   * @param fromNodeId the source node of this edge
   * @param toNodeId the destination node of this edge
   * @param tags the tags associated with this edge
   */
  @JsonCreator
  public Edge(@JsonProperty("itemId") long id,
               @JsonProperty("name") String name,
               @JsonProperty("sourceKey") String sourceKey,
               @JsonProperty("fromNodeId") long fromNodeId,
               @JsonProperty("toNodeId") long toNodeId,
               @JsonProperty("tags") Map<String, Tag> tags) {

    super(id, tags);

    this.name = name;
    this.fromNodeId = fromNodeId;
    this.toNodeId = toNodeId;
    this.sourceKey = sourceKey;
  }

  public Edge(long id, Edge other) {
    super(id, other.getTags());

    this.name = other.name;
    this.fromNodeId = other.fromNodeId;
    this.toNodeId = other.toNodeId;
    this.sourceKey = other.sourceKey;
  }

  public String getName() {
    return this.name;
  }

  public long getFromNodeId() {
    return this.fromNodeId;
  }

  public long getToNodeId() {
    return this.toNodeId;
  }

  public String getSourceKey() {
    return this.sourceKey;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Edge)) {
      return false;
    }

    Edge otherEdge = (Edge) other;

    return this.name.equals(otherEdge.name)
             && this.sourceKey.equals(otherEdge.sourceKey)
             && this.getId() == otherEdge.getId()
             && this.fromNodeId == otherEdge.fromNodeId
             && this.toNodeId == otherEdge.toNodeId
             && this.getTags().equals(otherEdge.getTags());
  }
}
