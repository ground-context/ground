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

package edu.berkeley.ground.api.usage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.berkeley.ground.api.versions.Item;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LineageEdge extends Item<LineageEdgeVersion> {
  private static final Logger LOGGER = LoggerFactory.getLogger(LineageEdge.class);

  // the name of this LineageEdge
  private String name;

  @JsonCreator
  protected LineageEdge(@JsonProperty("id") long id,
                        @JsonProperty("name") String name) {
    super(id);

    this.name = name;
  }

  @JsonProperty
  public String getName() {
    return this.name;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof LineageEdge)) {
      return false;
    }

    LineageEdge otherLineageEdge = (LineageEdge) other;

    return this.name.equals(otherLineageEdge.name) && this.getId() == otherLineageEdge.getId();
  }
}
