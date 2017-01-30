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

package edu.berkeley.ground.api.usage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class LineageEdgeVersion extends RichVersion {
    private static final Logger LOGGER = LoggerFactory.getLogger(LineageEdgeVersion.class);

    // the id of the LineageEdge containing this Version
    private String lineageEdgeId;

    // the id of the RichVersion that this LineageEdgeVersion originates from
    private String fromId;

    // the id of the RichVersion that this LineageEdgeVersion points to
    private String toId;

    @JsonCreator
    protected LineageEdgeVersion(@JsonProperty("id") String id,
                                 @JsonProperty("tags") Map<String, Tag> tags,
                                 @JsonProperty("structureVersionId") String structureVersionId,
                                 @JsonProperty("reference") String reference,
                                 @JsonProperty("referenceParameters") Map<String, String> referenceParameters,
                                 @JsonProperty("fromId") String fromId,
                                 @JsonProperty("toId") String toId,
                                 @JsonProperty("lineageEdgeId") String lineageEdgeId) {
        super(id, tags, structureVersionId, reference, referenceParameters);

        this.lineageEdgeId = lineageEdgeId;
        this.fromId = fromId;
        this.toId = toId;
    }

    @JsonProperty
    public String getLineageEdgeId() {
        return this.lineageEdgeId;
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
        if (!(other instanceof LineageEdgeVersion)) {
            return false;
        }

        LineageEdgeVersion otherLineageEdgeVersion = (LineageEdgeVersion) other;

        return this.lineageEdgeId.equals(otherLineageEdgeVersion.lineageEdgeId) &&
                this.fromId.equals(otherLineageEdgeVersion.fromId) &&
                this.toId.equals(otherLineageEdgeVersion.toId) &&
                this.getId().equals(otherLineageEdgeVersion.getId()) &&
                super.equals(other);
    }
}
