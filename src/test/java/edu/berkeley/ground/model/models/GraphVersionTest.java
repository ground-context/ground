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

import java.util.*;

import static io.dropwizard.testing.FixtureHelpers.fixture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

public class GraphVersionTest {
  private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

  @Test
  public void serializesToJSON() throws Exception {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

    Map<String, String> parametersMap = new HashMap<>();
    parametersMap.put("http", "GET");

    List<Long> edgeVersionIds = new ArrayList<>();
    edgeVersionIds.add(123L);
    edgeVersionIds.add(456L);

    GraphVersion graphVersion = new GraphVersion(1, tagsMap, -1, "http://www.google.com", parametersMap, 1, edgeVersionIds);

    final String expected = MAPPER.writeValueAsString(MAPPER.readValue(fixture("fixtures/models/graph_version.json"), GraphVersion.class));
    assertThat(MAPPER.writeValueAsString(graphVersion)).isEqualTo(expected);
  }

  @Test
  public void deserializesFromJSON() throws Exception {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

    Map<String, String> parametersMap = new HashMap<>();
    parametersMap.put("http", "GET");

    List<Long> edgeVersionIds = new ArrayList<>();
    edgeVersionIds.add(123L);
    edgeVersionIds.add(456L);

    GraphVersion graphVersion = new GraphVersion(1, tagsMap, -1, "http://www.google.com", parametersMap, 1, edgeVersionIds);

    assertEquals(MAPPER.readValue(fixture("fixtures/models/graph_version.json"), GraphVersion.class), graphVersion);
  }

  @Test
  public void testGraphVersionNotEquals() throws Exception {
    GraphVersion truth = new GraphVersion(1, new HashMap<>(), 2, "http://www.google.com",
        new HashMap<>(), 3, new ArrayList<>());
    assertFalse(truth.equals("notGraphVersion"));

    GraphVersion differentId = new GraphVersion(2, new HashMap<>(), 2, "http://www.google.com",
        new HashMap<>(), 3, new ArrayList<>());
    assertFalse(truth.equals(differentId));

    Map<String, Tag> tags = new HashMap<>();
    tags.put("test", new Tag(1, "test", 1L, GroundType.LONG));
    GraphVersion differentTags = new GraphVersion(1, tags, 2, "http://www.google.com",
        new HashMap<>(), 3, new ArrayList<>());
    assertFalse(truth.equals(differentTags));

    GraphVersion differenStructureVersionId = new GraphVersion(1, new HashMap<>(), 10,
        "http://www.google.com", new HashMap<>(), 3, new ArrayList<>());
    assertFalse(truth.equals(differenStructureVersionId));

    GraphVersion differentReference = new GraphVersion(1, new HashMap<>(), 2, "http://www.fb.com",
        new HashMap<>(), 3, new ArrayList<>());
    assertFalse(truth.equals(differentReference));

    Map<String, String> parameters = new HashMap<>();
    parameters.put("test", "param");
    GraphVersion differentParameters = new GraphVersion(1, new HashMap<>(), 2,
        "http://www.google.com", parameters, 3, new ArrayList<>());
    assertFalse(truth.equals(differentParameters));

    GraphVersion differentGraphId = new GraphVersion(1, new HashMap<>(), 2, "http://www.google.com",
        new HashMap<>(), 10, new ArrayList<>());
    assertFalse(truth.equals(differentGraphId));

    List<Long> ids = new ArrayList<>();
    ids.add(10L);
    GraphVersion differentEdgeVersionIds = new GraphVersion(1, new HashMap<>(), 2,
        "http://www.google.com", new HashMap<>(), 3, ids);
    assertFalse(truth.equals(differentEdgeVersionIds));
  }
}
