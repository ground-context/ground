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

package edu.berkeley.ground.model.usage;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import io.dropwizard.jackson.Jackson;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static io.dropwizard.testing.FixtureHelpers.fixture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

public class LineageEdgeVersionTest {
  private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

  @Test
  public void serializesToJSON() throws Exception {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

    Map<String, String> parametersMap = new HashMap<>();
    parametersMap.put("http", "GET");

    LineageEdgeVersion lineageEdgeVersion = new LineageEdgeVersion(1, tagsMap, -1, "http://www.google.com", parametersMap, 123, 456, 1);

    final String expected = MAPPER.writeValueAsString(MAPPER.readValue(fixture("fixtures/usage/lineage_edge_version.json"), LineageEdgeVersion.class));
    assertThat(MAPPER.writeValueAsString(lineageEdgeVersion)).isEqualTo(expected);
  }

  @Test
  public void deserializesFromJSON() throws Exception {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

    Map<String, String> parametersMap = new HashMap<>();
    parametersMap.put("http", "GET");

    LineageEdgeVersion lineageEdgeVersion = new LineageEdgeVersion(1, tagsMap, -1, "http://www.google.com", parametersMap, 123, 456, 1);

    assertEquals(MAPPER.readValue(fixture("fixtures/usage/lineage_edge_version.json"), LineageEdgeVersion.class), lineageEdgeVersion);
  }

  @Test
  public void testLineageEdgeVersionNotEquals() throws Exception {
    LineageEdgeVersion truth = new LineageEdgeVersion(1, new HashMap<>(), 2,
        "http://www.google.com", new HashMap<>(), 3, 4, 5);
    assertFalse(truth.equals("notEdgeVersion"));

    LineageEdgeVersion differentId = new LineageEdgeVersion(2, new HashMap<>(), 2,
        "http://www.google.com", new HashMap<>(), 3, 4, 5);
    assertFalse(truth.equals(differentId));

    Map<String, Tag> tags = new HashMap<>();
    tags.put("test", new Tag(1, "test", 1L, GroundType.LONG));
    LineageEdgeVersion differentTags = new LineageEdgeVersion(1, tags, 2, "http://www.google.com",
        new HashMap<>(), 3, 4, 5);
    assertFalse(truth.equals(differentTags));

    LineageEdgeVersion differenStructureVersionId = new LineageEdgeVersion(1, new HashMap<>(), 10,
        "http://www.google.com", new HashMap<>(), 3, 4, 5);
    assertFalse(truth.equals(differenStructureVersionId));

    LineageEdgeVersion differentReference = new LineageEdgeVersion(1, new HashMap<>(), 2,
        "http://www.fb.com", new HashMap<>(), 3, 4, 5);
    assertFalse(truth.equals(differentReference));

    Map<String, String> parameters = new HashMap<>();
    parameters.put("test", "param");
    LineageEdgeVersion differentParameters = new LineageEdgeVersion(1, new HashMap<>(), 2,
        "http://www.google.com", parameters, 3, 4, 5);
    assertFalse(truth.equals(differentParameters));

    LineageEdgeVersion differentEdgeId = new LineageEdgeVersion(1, new HashMap<>(), 2,
        "http://www.google.com", new HashMap<>(), 10, 4, 5);
    assertFalse(truth.equals(differentEdgeId));

    LineageEdgeVersion differentFromVersion = new LineageEdgeVersion(1, new HashMap<>(), 2,
        "http://www.google.com", new HashMap<>(), 3, 10, 5);
    assertFalse(truth.equals(differentFromVersion));

    LineageEdgeVersion differentToVersion = new LineageEdgeVersion(1, new HashMap<>(), 2,
        "http://www.google.com", new HashMap<>(), 3, 4, 10);
    assertFalse(truth.equals(differentToVersion));
  }
}
