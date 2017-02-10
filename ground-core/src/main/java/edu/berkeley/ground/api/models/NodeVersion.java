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

import java.util.*;

public class NodeVersion extends RichVersion {
  // the id of the Node containing this Version
  private long nodeId;

  @JsonCreator
  public NodeVersion(
      @JsonProperty("id") long id,
      @JsonProperty("tags") Map<String, Tag> tags,
      @JsonProperty("structureVersionId") long structureVersionId,
      @JsonProperty("reference") String reference,
      @JsonProperty("referenceParameters") Map<String, String> referenceParameters,
      @JsonProperty("nodeId") long nodeId) {

    super(id, tags, structureVersionId, reference, referenceParameters);

    this.nodeId = nodeId;
  }

  @JsonProperty
  public long getNodeId() {
    return this.nodeId;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof NodeVersion)) {
      return false;
    }

    NodeVersion otherNodeVersion = (NodeVersion) other;

    return this.nodeId == otherNodeVersion.nodeId && super.equals(other);
  }
}
