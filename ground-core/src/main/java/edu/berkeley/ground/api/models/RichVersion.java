package edu.berkeley.ground.api.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.berkeley.ground.api.versions.Version;
import org.hibernate.validator.valuehandling.UnwrapValidatedValue;

import java.util.*;

public class RichVersion extends Version {
    // the map of Keys to Tags associated with this RichVersion
    private Optional<Map<String, Tag>> tags;

    @UnwrapValidatedValue
    // the optional StructureVersion associated with this RichVersion
    private Optional<String> structureVersionId;

    @UnwrapValidatedValue
    // the optional reference associated with this RichVersion
    private Optional<String> reference;

    @UnwrapValidatedValue
    // the optional parameters associated with this RichVersion if there is a reference
    private Optional<Map<String, String>> parameters;

    protected RichVersion(String id,
                          Optional<Map<String, Tag>> tags,
                          Optional<String> structureVersionId,
                          Optional<String> reference,
                          Optional<Map<String, String>> parameters) {

        super(id);

        this.tags = tags;
        this.structureVersionId = structureVersionId;
        this.reference = reference;
        this.parameters = parameters;
    }

    @JsonProperty
    public Optional<Map<String, Tag>> getTags() {
        return this.tags;
    }

    @JsonProperty
    public Optional<String> getStructureVersionId() {
        return this.structureVersionId;
    }

    @JsonProperty
    public Optional<String> getReference() {
        return this.reference;
    }

    @JsonProperty
    public Optional<Map<String, String>> getParameters() {
        return this.parameters;
    }

}
