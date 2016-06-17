package edu.berkeley.ground.api.models;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import static io.dropwizard.testing.FixtureHelpers.fixture;
import static org.assertj.core.api.Assertions.assertThat;

public class GraphTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void serializesToJSON() throws Exception {
        Graph graph = new Graph("Graphs.test", "test");
        String expected = MAPPER.writeValueAsString(MAPPER.readValue(fixture("fixtures/models/graph.json"), Graph.class));

        assertThat(MAPPER.writeValueAsString(graph)).isEqualTo(expected);
    }

    @Test
    public void deserializesFromJSON() throws Exception {
        Graph graph = new Graph("Graphs.test", "test");
        assertThat(MAPPER.readValue(fixture("fixtures/models/graph.json"), Graph.class)).isEqualToComparingFieldByField(graph);
    }
}
