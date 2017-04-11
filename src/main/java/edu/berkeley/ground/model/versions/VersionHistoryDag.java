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

package edu.berkeley.ground.model.versions;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class VersionHistoryDag<T extends Version> {
  // the id of the Version that's at the rootId of this DAG
  private final long itemId;

  // list of VersionSuccessors that make up this DAG
  private final List<Long> edgeIds;

  // map of parents to children
  private final Map<Long, List<Long>> parentChildMap;

  /**
   * Create a new version history DAG.
   *
   * @param itemId the id of the item of this DAG
   * @param edges the version successors in this DAG
   */
  public VersionHistoryDag(long itemId, List<VersionSuccessor<T>> edges) {
    this.itemId = itemId;
    this.edgeIds = edges.stream().map(VersionSuccessor::getId)
        .collect(Collectors.toList());
    this.parentChildMap = new HashMap<>();

    edges.forEach(edge -> this.addToParentChildMap(edge.getFromId(), edge.getToId()));
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
   * @param childId the id of the "to" of the edge
   */
  public void addEdge(long parentId, long childId, long successorId) {
    this.edgeIds.add(successorId);
    this.addToParentChildMap(parentId, childId);
  }

  /**
   * Return the parent(s) of a particular version.
   *
   * @param childId the query id
   * @return the list of parent version(s)
   */
  public List<Long> getParent(long childId) {
    return this.parentChildMap.entrySet().stream()
        .filter(entry -> entry.getValue().contains(childId))
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
  }

  /**
   * Returns the leaves of the DAG (i.e., any version id that is not a parent of another version
   * id).
   *
   * @return the list of the IDs of the leaves of this DAG
   */
  public List<Long> getLeaves() {
    Set<Long> leaves = new HashSet<>();
    this.parentChildMap.values().forEach(leaves::addAll);
    leaves.removeAll(this.parentChildMap.keySet());

    return new ArrayList<>(leaves);
  }

  private void addToParentChildMap(long parent, long child) {
    List<Long> childList = this.parentChildMap.computeIfAbsent(parent,
        key -> new ArrayList<>());
    childList.add(child);
  }
}
