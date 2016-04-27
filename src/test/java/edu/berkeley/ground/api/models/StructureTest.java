package edu.berkeley.ground.api.models;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import static io.dropwizard.testing.FixtureHelpers.fixture;
import static org.assertj.core.api.Assertions.assertThat;

public class StructureTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void serializesToJSON() throws Exception {
        Structure structure = new Structure("Structures.test", "test");
        final String expected = MAPPER.writeValueAsString(MAPPER.readValue(fixture("fixtures/models/structure.json"), Structure.class));

        assertThat(MAPPER.writeValueAsString(structure)).isEqualTo(expected);
    }

    @Test
    public void deserializesFromJSON() throws Exception {
        Structure structure = new Structure("Structures.test", "test");
        assertThat(MAPPER.readValue(fixture("fixtures/models/structure.json"), Structure.class)).isEqualToComparingFieldByField(structure);
    }
}
