/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.model.models;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.berkeley.ground.model.versions.GroundType;
import io.dropwizard.jackson.Jackson;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static io.dropwizard.testing.FixtureHelpers.fixture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

public class EdgeVersionTest {
  private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

  @Test
  public void serializesToJSON() throws Exception {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

    Map<String, String> parametersMap = new HashMap<>();
    parametersMap.put("http", "GET");

    EdgeVersion edgeVersion = new EdgeVersion(1, tagsMap, -1, "http://www.google.com",
        parametersMap, 1, 123, -1, 456, -1);

    final String expected = MAPPER.writeValueAsString(MAPPER.readValue(fixture("fixtures/models/edge_version.json"), EdgeVersion.class));
    assertThat(MAPPER.writeValueAsString(edgeVersion)).isEqualTo(expected);
  }

  @Test
  public void deserializesFromJSON() throws Exception {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

    Map<String, String> parametersMap = new HashMap<>();
    parametersMap.put("http", "GET");

    EdgeVersion edgeVersion = new EdgeVersion(1, tagsMap, -1, "http://www.google.com",
        parametersMap, 1, 123, -1, 456, -1);

    assertEquals(MAPPER.readValue(fixture("fixtures/models/edge_version.json"), EdgeVersion.class), edgeVersion);
  }

  @Test
  public void testEdgeVersionNotEquals() throws Exception {
    EdgeVersion truth = new EdgeVersion(1, new HashMap<>(), 2, "http://www.google.com",
        new HashMap<>(), 3, 4, 5, 6, 7);
    assertFalse(truth.equals("notEdgeVersion"));

    EdgeVersion differentId = new EdgeVersion(2, new HashMap<>(), 2, "http://www.google.com",
        new HashMap<>(), 3, 4, 5, 6, 7);
    assertFalse(truth.equals(differentId));

    Map<String, Tag> tags = new HashMap<>();
    tags.put("test", new Tag(1, "test", 1L, GroundType.LONG));
    EdgeVersion differentTags = new EdgeVersion(1, tags, 2, "http://www.google.com",
        new HashMap<>(), 3, 4, 5, 6, 7);
    assertFalse(truth.equals(differentTags));

    EdgeVersion differenStructureVersionId = new EdgeVersion(1, new HashMap<>(), 10,
        "http://www.google.com", new HashMap<>(), 3, 4, 5, 6, 7);
    assertFalse(truth.equals(differenStructureVersionId));

    EdgeVersion differentReference = new EdgeVersion(1, new HashMap<>(), 2, "http://www.fb.com",
        new HashMap<>(), 3, 4, 5, 6, 7);
    assertFalse(truth.equals(differentReference));

    Map<String, String> parameters = new HashMap<>();
    parameters.put("test", "param");
    EdgeVersion differentParameters = new EdgeVersion(1, new HashMap<>(), 2,
        "http://www.google.com", parameters, 3, 4, 5, 6, 7);
    assertFalse(truth.equals(differentParameters));

    EdgeVersion differentEdgeId = new EdgeVersion(1, new HashMap<>(), 2, "http://www.google.com",
        new HashMap<>(), 10, 4, 5, 6, 7);
    assertFalse(truth.equals(differentEdgeId));

    EdgeVersion differentFromStart = new EdgeVersion(1, new HashMap<>(), 2, "http://www.google.com",
        new HashMap<>(), 3, 10, 5, 6, 7);
    assertFalse(truth.equals(differentFromStart));

    EdgeVersion differentFromEnd = new EdgeVersion(1, new HashMap<>(), 2, "http://www.google.com",
        new HashMap<>(), 3, 4, 10, 6, 7);
    assertFalse(truth.equals(differentFromEnd));

    EdgeVersion differentToStart = new EdgeVersion(1, new HashMap<>(), 2, "http://www.google.com",
        new HashMap<>(), 3, 4, 5, 10, 7);
    assertFalse(truth.equals(differentToStart));

    EdgeVersion differentToEnd = new EdgeVersion(1, new HashMap<>(), 2, "http://www.google.com",
        new HashMap<>(), 3, 4, 5, 6, 10);
    assertFalse(truth.equals(differentToEnd));
  }
}
