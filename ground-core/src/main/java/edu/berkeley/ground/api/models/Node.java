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

import edu.berkeley.ground.api.versions.Item;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class Node extends Item<NodeVersion> {
  // the name of this Node
  private String name;

  @JsonCreator
  public Node(
      @JsonProperty("id") long id,
      @JsonProperty("name") String name,
      @JsonProperty("tags") Map<String, Tag> tags) {
    super(id, tags);

    this.name = name;
  }

  @JsonProperty
  public String getName() {
    return this.name;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof Node)) {
      return false;
    }

    Node otherNode = (Node) other;

    return this.name.equals(otherNode.name) && this.getId() == otherNode.getId();
  }
}
