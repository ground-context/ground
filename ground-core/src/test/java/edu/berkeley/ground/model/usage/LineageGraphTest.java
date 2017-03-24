package edu.berkeley.ground.model.usage;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import io.dropwizard.jackson.Jackson;

import static io.dropwizard.testing.FixtureHelpers.fixture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

public class LineageGraphTest {
  private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

  @Test
  public void serializesToJSON() throws Exception {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

    LineageGraph lineageGraph = new LineageGraph(1, "test", "testKey", tagsMap);
    String expected = MAPPER.writeValueAsString(MAPPER.readValue(fixture
        ("fixtures/usage/lineage_graph.json"), LineageGraph.class));

    assertThat(MAPPER.writeValueAsString(lineageGraph)).isEqualTo(expected);
  }

  @Test
  public void deserializesFromJSON() throws Exception {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

    LineageGraph lineageGraph = new LineageGraph(1, "test", "testKey", tagsMap);
    assertEquals(MAPPER.readValue(fixture("fixtures/usage/lineage_graph.json"),
        LineageGraph.class), lineageGraph);
  }
}
