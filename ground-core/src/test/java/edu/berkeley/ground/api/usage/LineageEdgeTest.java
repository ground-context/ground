package edu.berkeley.ground.api.usage;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import static io.dropwizard.testing.FixtureHelpers.fixture;
import static org.assertj.core.api.Assertions.assertThat;

public class LineageEdgeTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void serializesToJSON() throws Exception {
        LineageEdge lineageEdge = new LineageEdge("LineageEdges.test", "test");
        String expected = MAPPER.writeValueAsString(MAPPER.readValue(fixture("fixtures/usage/lineage_edge.json"), LineageEdge.class));

        assertThat(MAPPER.writeValueAsString(lineageEdge)).isEqualTo(expected);
    }

    @Test
    public void deserializesFromJSON() throws Exception {
        LineageEdge lineageEdge = new LineageEdge("LineageEdges.test", "test");
        assertThat(MAPPER.readValue(fixture("fixtures/usage/lineage_edge.json"), LineageEdge.class)).isEqualToComparingFieldByField(lineageEdge);
    }
}
