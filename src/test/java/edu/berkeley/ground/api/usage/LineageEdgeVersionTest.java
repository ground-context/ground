package edu.berkeley.ground.api.usage;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.Type;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.dropwizard.testing.FixtureHelpers.fixture;
import static org.assertj.core.api.Assertions.assertThat;

public class LineageEdgeVersionTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void serializesToJSON() throws Exception {
        Map<String, Tag> tagsMap = new HashMap<>();
        tagsMap.put("testtag", new Tag("abcd", "testtag", Optional.of("tag"), Optional.of(Type.STRING)));

        Map<String, String> parametersMap = new HashMap<>();
        parametersMap.put("http", "GET");

        LineageEdgeVersion lineageEdgeVersion = new LineageEdgeVersion("abcd", Optional.of(tagsMap), Optional.<String>empty(), Optional.of("http://www.google.com"), Optional.of(parametersMap),  "123", "456", "LineageEdges.test");

        final String expected = MAPPER.writeValueAsString(MAPPER.readValue(fixture("fixtures/usage/lineage_edge_version.json"), LineageEdgeVersion.class));
        assertThat(MAPPER.writeValueAsString(lineageEdgeVersion)).isEqualTo(expected);
    }

    @Test
    public void deserializesFromJSON() throws Exception {
        Map<String, Tag> tagsMap = new HashMap<>();
        tagsMap.put("testtag", new Tag("abcd", "testtag", Optional.of("tag"), Optional.of(Type.STRING)));

        Map<String, String> parametersMap = new HashMap<>();
        parametersMap.put("http", "GET");

        LineageEdgeVersion lineageEdgeVersion = new LineageEdgeVersion("abcd", Optional.of(tagsMap), Optional.<String>empty(), Optional.of("http://www.google.com"), Optional.of(parametersMap),  "123", "456", "LineageEdges.test");

        assertThat(MAPPER.readValue(fixture("fixtures/usage/lineage_edge_version.json"), LineageEdgeVersion.class)).isEqualToComparingFieldByField(lineageEdgeVersion);
    }
}
