package edu.berkeley.ground.api.models;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import static io.dropwizard.testing.FixtureHelpers.fixture;
import static org.assertj.core.api.Assertions.assertThat;

public class NodeTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void serializesToJSON() throws Exception {
        final Node node = new Node("Nodes.test", "test");
        final String expected = MAPPER.writeValueAsString(MAPPER.readValue(fixture("fixtures/models/node.json"), Node.class));

        assertThat(MAPPER.writeValueAsString(node)).isEqualTo(expected);
    }

    @Test
    public void deserializesFromJSON() throws Exception {
        final Node node = new Node("Nodes.test", "test");
        assertThat(MAPPER.readValue(fixture("fixtures/models/node.json"), Node.class)).isEqualToComparingFieldByField(node);
    }
}
