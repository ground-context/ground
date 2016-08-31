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
import edu.berkeley.ground.api.versions.GroundType;
import org.hibernate.validator.constraints.NotEmpty;
import org.hibernate.validator.valuehandling.UnwrapValidatedValue;

import java.util.*;

public class Tag {
    private String versionId;

    @NotEmpty
    // the Key of the Tag
    private String key;

    @UnwrapValidatedValue
    // the optional Value of the Tag
    private Optional<Object> value;

    @UnwrapValidatedValue
    // the Type of the Value if it exists
    private Optional<GroundType> valueType;

    @JsonCreator
    public Tag(@JsonProperty("versionId") String versionId,
               @JsonProperty("key") String key,
               @JsonProperty("value") Optional<Object> value,
                @JsonProperty("type") Optional<GroundType> valueType) {
        this.versionId = versionId;
        this.key = key;
        this.value = value;
        this.valueType = valueType;
    }

    @JsonProperty
    public String getVersionId() {
        return this.versionId;
    }

    @JsonProperty
    public String getKey() {
        return this.key;
    }

    @JsonProperty
    public Optional<Object> getValue() {
        return this.value;
    }

    @JsonProperty
    public Optional<GroundType> getValueType() {
        return this.valueType;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Tag)) {
            return false;
        }

        Tag that = (Tag) other;

        return this.key.equals(that.key) && this.value.equals(that.value) && this.valueType.equals(that.valueType);
    }
}
