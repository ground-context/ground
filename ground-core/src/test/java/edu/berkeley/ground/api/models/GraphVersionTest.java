/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.api.models;

import com.fasterxml.jackson.databind.ObjectMapper;

import edu.berkeley.ground.api.versions.GroundType;
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

    assertThat(MAPPER.readValue(fixture("fixtures/models/graph_version.json"), GraphVersion.class)).isEqualToComparingFieldByField(graphVersion);
  }
}
