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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class GraphVersion extends RichVersion {
    private static final Logger LOGGER = LoggerFactory.getLogger(GraphVersion.class);

    // the id of the Graph that contains this Version
    private String graphId;

    // the list of ids of EdgeVersions in this GraphVersion
    private List<String> edgeVersionIds;

    @JsonCreator
    protected GraphVersion(@JsonProperty("id") String id,
                           @JsonProperty("tags") Map<String, Tag> tags,
                           @JsonProperty("structureVersionId") String structureVersionId,
                           @JsonProperty("reference") String reference,
                           @JsonProperty("parameters") Map<String, String> parameters,
                           @JsonProperty("graphId") String graphId,
                           @JsonProperty("edgeVersionIds") List<String> edgeVersionIds)  {

        super(id, tags, structureVersionId, reference, parameters);

        this.graphId = graphId;
        this.edgeVersionIds = edgeVersionIds;
    }

    @JsonProperty
    public String getGraphId() {
        return this.graphId;
    }

    @JsonProperty
    public List<String> getEdgeVersionIds() {
        return this.edgeVersionIds;
    }

}
