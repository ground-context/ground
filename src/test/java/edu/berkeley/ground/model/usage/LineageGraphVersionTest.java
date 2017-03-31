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

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import io.dropwizard.jackson.Jackson;

import static io.dropwizard.testing.FixtureHelpers.fixture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

public class LineageGraphVersionTest {
  private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

  @Test
  public void serializesToJSON() throws Exception {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

    Map<String, String> parametersMap = new HashMap<>();
    parametersMap.put("http", "GET");

    List<Long> lineagedgeVersionIds = new ArrayList<>();
    lineagedgeVersionIds.add(123L);
    lineagedgeVersionIds.add(456L);

    LineageGraphVersion graphVersion = new LineageGraphVersion(1, tagsMap, -1,
        "http://www.google.com", parametersMap, 1, lineagedgeVersionIds);

    final String expected = MAPPER.writeValueAsString(MAPPER.readValue(
        fixture("fixtures/models/graph_version.json"), LineageGraphVersion.class));
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

    LineageGraphVersion lineageGraphVersion = new LineageGraphVersion(1, tagsMap, -1,
        "http://www.google.com", parametersMap, 1, edgeVersionIds);

    assertEquals(MAPPER.readValue(fixture("fixtures/models/graph_version.json"),
        LineageGraphVersion.class), lineageGraphVersion);
  }
}
