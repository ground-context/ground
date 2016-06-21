package edu.berkeley.ground.api.models;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.berkeley.ground.api.versions.Type;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static io.dropwizard.testing.FixtureHelpers.fixture;
import static org.assertj.core.api.Assertions.assertThat;

public class EdgeVersionTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void serializesToJSON() throws Exception {
        Map<String, Tag> tagsMap = new HashMap<>();
        tagsMap.put("testtag", new Tag("abcd", "testtag", Optional.of("tag"), Optional.of(Type.STRING)));

        Map<String, String> parametersMap = new HashMap<>();
        parametersMap.put("http", "GET");

        EdgeVersion edgeVersion = new EdgeVersion("abcd", Optional.of(tagsMap), Optional.<String>empty(), Optional.of("http://www.google.com"), Optional.of(parametersMap), "Edges.test", "123", "456");

        final String expected = MAPPER.writeValueAsString(MAPPER.readValue(fixture("fixtures/models/edge_version.json"), EdgeVersion.class));
        assertThat(MAPPER.writeValueAsString(edgeVersion)).isEqualTo(expected);
    }

    @Test
    public void deserializesFromJSON() throws Exception {
        Map<String, Tag> tagsMap = new HashMap<>();
        tagsMap.put("testtag", new Tag("abcd", "testtag", Optional.of("tag"), Optional.of(Type.STRING)));

        Map<String, String> parametersMap = new HashMap<>();
        parametersMap.put("http", "GET");

        EdgeVersion edgeVersion = new EdgeVersion("abcd", Optional.of(tagsMap), Optional.<String>empty(), Optional.of("http://www.google.com"), Optional.of(parametersMap), "Edges.test", "123", "456");

        assertThat(MAPPER.readValue(fixture("fixtures/models/edge_version.json"), EdgeVersion.class)).isEqualToComparingFieldByField(edgeVersion);
    }
}
