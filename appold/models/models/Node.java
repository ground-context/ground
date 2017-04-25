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

package models.models;




import models.versions.Item;

import java.util.Map;

public class Node extends Item<NodeVersion> {
  // the name of this Node
  private final String name;

  // the source key for this Node
  private final String sourceKey;

  /**
   * Create a new node.
   *
   * @param id the id of the node
   * @param name the name of the node
   * @param sourceKey the user-generated source key of the node
   * @param tags the tags associated with the node
   */
  
  public Node(
      ("id") long id,
      ("name") String name,
      ("sourceKey") String sourceKey,
      ("tags") Map<String, Tag> tags) {
    super(id, tags);

    this.name = name;
    this.sourceKey = sourceKey;
  }

  
  public String getName() {
    return this.name;
  }

  
  public String getSourceKey() {
    return this.sourceKey;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Node)) {
      return false;
    }

    Node otherNode = (Node) other;

    return this.name.equals(otherNode.name)
        && this.getId() == otherNode.getId()
        && this.sourceKey.equals(otherNode.sourceKey)
        && this.getTags().equals(otherNode.getTags());
  }
}
