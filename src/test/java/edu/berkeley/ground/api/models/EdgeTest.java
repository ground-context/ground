package edu.berkeley.ground.api.models;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import static io.dropwizard.testing.FixtureHelpers.fixture;
import static org.assertj.core.api.Assertions.assertThat;

public class EdgeTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void serializesToJSON() throws Exception {
        final Edge edge = new Edge("Edges.test", "test");
        final String expected = MAPPER.writeValueAsString(MAPPER.readValue(fixture("fixtures/models/edge.json"), Edge.class));

        assertThat(MAPPER.writeValueAsString(edge)).isEqualTo(expected);
    }

    @Test
    public void deserializesFromJSON() throws Exception {
        final Edge edge = new Edge("Edges.test", "test");
        assertThat(MAPPER.readValue(fixture("fixtures/models/edge.json"), Edge.class)).isEqualToComparingFieldByField(edge);
    }
}
