/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
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

public class Node extends Item<NodeVersion> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Node.class);

    // the name of this Node
    private String name;

    @JsonCreator
    public Node(
            @JsonProperty("id") String id,
            @JsonProperty("name") String name) {
        super(id);

        this.name = name;
    }

    @JsonProperty
    public String getName() {
        return this.name;
    }

    public static String idToName(String id) {
        return id.substring(6);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Node)) {
            return false;
        }

        Node otherNode = (Node) other;

        return this.name.equals(otherNode.name) && this.getId().equals(otherNode.getId());
    }
}
