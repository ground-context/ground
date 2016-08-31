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

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.berkeley.ground.api.versions.Version;
import org.hibernate.validator.valuehandling.UnwrapValidatedValue;

import java.util.*;

public class RichVersion extends Version {
    // the map of Keys to Tags associated with this RichVersion
    private Map<String, Tag> tags;

    @UnwrapValidatedValue
    // the StructureVersion associated with this RichVersion
    private String structureVersionId;

    @UnwrapValidatedValue
    // the optional reference associated with this RichVersion
    private String reference;

    @UnwrapValidatedValue
    // the optional parameters associated with this RichVersion if there is a reference
    private Map<String, String> parameters;

    protected RichVersion(String id,
                          Map<String, Tag> tags,
                          String structureVersionId,
                          String reference,
                          Map<String, String> parameters) {

        super(id);

        this.tags = tags;
        this.structureVersionId = structureVersionId;
        this.reference = reference;
        this.parameters = parameters;
    }

    @JsonProperty
    public Map<String, Tag> getTags() {
        return this.tags;
    }

    @JsonProperty
    public String getStructureVersionId() {
        return this.structureVersionId;
    }

    @JsonProperty
    public String getReference() {
        return this.reference;
    }

    @JsonProperty
    public Map<String, String> getParameters() {
        return this.parameters;
    }

}
