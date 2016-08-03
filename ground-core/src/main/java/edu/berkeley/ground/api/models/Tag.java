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
