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
import java.util.Optional;

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
                                 @JsonProperty("tags") Optional<Map<String, Tag>> tags,
                                 @JsonProperty("structureVersionId") Optional<String> structureVersionId,
                                 @JsonProperty("reference") Optional<String> reference,
                                 @JsonProperty("parameters") Optional<Map<String, String>> parameters,
                                 @JsonProperty("fromId") String fromId,
                                 @JsonProperty("toId") String toId,
                                 @JsonProperty("lineageEdgeId") String lineageEdgeId) {
        super(id, tags, structureVersionId, reference, parameters);

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

}
