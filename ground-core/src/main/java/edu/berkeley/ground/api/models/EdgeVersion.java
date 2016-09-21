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

import java.util.Map;
import java.util.Optional;

public class EdgeVersion extends RichVersion {
    // the id of the Edge containing this Version
    private String edgeId;

    // the id of the NodeVersion that this EdgeVersion originates from
    private String fromId;

    // the id of the NodeVersion that this EdgeVersion points to
    private String toId;

    @JsonCreator
    protected EdgeVersion(
            @JsonProperty("id") String id,
            @JsonProperty("tags") Map<String, Tag> tags,
            @JsonProperty("structureVersionId") String structureVersionId,
            @JsonProperty("reference") String reference,
            @JsonProperty("referenceParameters") Map<String, String> referenceParameters,
            @JsonProperty("edgeId") String edgeId,
            @JsonProperty("fromId") String fromId,
            @JsonProperty("toId") String toId) {

        super(id, tags, structureVersionId, reference, referenceParameters);

        this.edgeId = edgeId;
        this.fromId = fromId;
        this.toId = toId;
    }

    @JsonProperty
    public String getEdgeId() {
        return this.edgeId;
    }

    @JsonProperty
    public String getFromId() {
        return this.fromId;
    }

    @JsonProperty
    public String getToId() {
        return this.toId;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof EdgeVersion)) {
            return false;
        }

        EdgeVersion otherEdgeVersion = (EdgeVersion) other;

        return this.edgeId.equals(otherEdgeVersion.edgeId) &&
                this.fromId.equals(otherEdgeVersion.fromId) &&
                this.toId.equals(otherEdgeVersion.toId) &&
                this.getId().equals(otherEdgeVersion.getId()) &&
                super.equals(other);
    }
}
