package edu.berkeley.ground.api.models;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.berkeley.ground.api.versions.Type;
import io.dropwizard.jackson.Jackson;
import org.junit.Test;

import java.util.*;

import static io.dropwizard.testing.FixtureHelpers.fixture;
import static org.assertj.core.api.Assertions.assertThat;

public class GraphVersionTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void serializesToJSON() throws Exception {
        Map<String, Tag> tagsMap = new HashMap<>();
        tagsMap.put("testtag", new Tag("abcd", "testtag", Optional.of("tag"), Optional.of(Type.STRING)));

        Map<String, String> parametersMap = new HashMap<>();
        parametersMap.put("http", "GET");

        List<String> edgeVersionIds = new ArrayList<>();
        edgeVersionIds.add("abc");
        edgeVersionIds.add("def");

        GraphVersion graphVersion = new GraphVersion("abcd", Optional.of(tagsMap), Optional.<String>empty(), Optional.of("http://www.google.com"), Optional.of(parametersMap), "Graphs.test", edgeVersionIds);

        final String expected = MAPPER.writeValueAsString(MAPPER.readValue(fixture("fixtures/models/graph_version.json"), GraphVersion.class));
        assertThat(MAPPER.writeValueAsString(graphVersion)).isEqualTo(expected);
    }

    @Test
    public void deserializesFromJSON() throws Exception {
        Map<String, Tag> tagsMap = new HashMap<>();
        tagsMap.put("testtag", new Tag("abcd", "testtag", Optional.of("tag"), Optional.of(Type.STRING)));

        Map<String, String> parametersMap = new HashMap<>();
        parametersMap.put("http", "GET");

        List<String> edgeVersionIds = new ArrayList<>();
        edgeVersionIds.add("abc");
        edgeVersionIds.add("def");

        GraphVersion graphVersion = new GraphVersion("abcd", Optional.of(tagsMap), Optional.<String>empty(), Optional.of("http://www.google.com"), Optional.of(parametersMap), "Graphs.test", edgeVersionIds);

        assertThat(MAPPER.readValue(fixture("fixtures/models/graph_version.json"), GraphVersion.class)).isEqualToComparingFieldByField(graphVersion);
    }
}
