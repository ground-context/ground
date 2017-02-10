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

package edu.berkeley.ground.api.versions;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.berkeley.ground.exceptions.GroundException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class VersionHistoryDAG<T extends Version> {
  // the id of the Version that's at the rootId of this DAG
  private long itemId;

  // list of VersionSuccessors that make up this DAG
  private List<Long> edgeIds;

  // map of parents to children
  private Map<Long, List<Long>> parentChildMap;

  protected VersionHistoryDAG(long itemId, List<VersionSuccessor<T>> edges) {
    this.itemId = itemId;
    this.edgeIds = new ArrayList<>();
    this.parentChildMap = new HashMap<>();

    for (VersionSuccessor<T> edge : edges) {
      edgeIds.add(edge.getId());

      this.addToParentChildMap(edge.getFromId(), edge.getToId());
    }
  }

  @JsonProperty
  public long getItemId() {
    return this.itemId;
  }

  @JsonProperty
  public List<Long> getEdgeIds() {
    return this.edgeIds;
  }

  /**
   * Checks if a given ID is in the DAG.
   *
   * @param id the ID to be checked
   * @return true if id is in the DAG, false otherwise
   */
  public boolean checkItemInDag(long id) {
    return this.parentChildMap.keySet().contains(id) || this.getLeaves().contains(id);
  }

  /**
   * Adds an edge to this DAG.
   *
   * @param parentId the id of the "from" of the edge
   * @param childId  the id of the "to" of the edge
   */
  public void addEdge(long parentId, long childId, long successorId) {
    edgeIds.add(successorId);
    this.addToParentChildMap(parentId, childId);
  }

  /**
   * Returns the leaves of the DAG (i.e., any version id that is not a parent of another version
   * id).
   *
   * @return the list of the IDs of the leaves of this DAG
   */
  public List<Long> getLeaves() {
    List<Long> leaves = new ArrayList<>();
    for (List<Long> values : parentChildMap.values()) {
      leaves.addAll(values);
    }

    leaves.removeAll(this.parentChildMap.keySet());

    Set<Long> leafSet = new HashSet<>(leaves);
    List<Long> result = new ArrayList<>();
    result.addAll(leafSet);

    return result;
  }

  private void addToParentChildMap(long parent, long child) {
    List<Long> childList = this.parentChildMap.get(parent);
    if (childList == null) {
      childList = new ArrayList<>();
    }

    childList.add(child);
    this.parentChildMap.put(parent, childList);
  }
}
