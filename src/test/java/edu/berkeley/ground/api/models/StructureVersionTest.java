package edu.berkeley.ground.api.models;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.berkeley.ground.api.versions.Type;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static io.dropwizard.testing.FixtureHelpers.fixture;
import static org.assertj.core.api.Assertions.assertThat;

public class StructureVersionTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void serializesToJSON() throws Exception {
        Map<String, Type> attributes = new HashMap<>();
        attributes.put("tag1", Type.INTEGER);
        attributes.put("tag2", Type.STRING);

        StructureVersion structureVersion = new StructureVersion("abcd", "Structures.test", attributes);

        final String expected = MAPPER.writeValueAsString(MAPPER.readValue(fixture("fixtures/models/structure_version.json"), StructureVersion.class));
        assertThat(MAPPER.writeValueAsString(structureVersion)).isEqualTo(expected);
    }

    @Test
    public void deserializesFromJSON() throws Exception {
        Map<String, Type> attributes = new HashMap<>();
        attributes.put("tag1", Type.INTEGER);
        attributes.put("tag2", Type.STRING);

        StructureVersion structureVersion = new StructureVersion("abcd", "Structures.test", attributes);

        assertThat(MAPPER.readValue(fixture("fixtures/models/structure_version.json"), StructureVersion.class)).isEqualToComparingFieldByField(structureVersion);
    }
}
