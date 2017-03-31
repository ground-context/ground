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

public class LineageEdgeTest {
  private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

  @Test
  public void serializesToJSON() throws Exception {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

    LineageEdge lineageEdge = new LineageEdge(1, "test", "testKey", tagsMap);
    String expected = MAPPER.writeValueAsString(MAPPER.readValue(fixture("fixtures/usage/lineage_edge.json"), LineageEdge.class));

    assertThat(MAPPER.writeValueAsString(lineageEdge)).isEqualTo(expected);
  }

  @Test
  public void deserializesFromJSON() throws Exception {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

    LineageEdge lineageEdge = new LineageEdge(1, "test", "testKey", tagsMap);
    assertEquals(MAPPER.readValue(fixture("fixtures/usage/lineage_edge.json"), LineageEdge.class), lineageEdge);
  }
}
