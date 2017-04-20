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

package models.models;

import java.util.Map;
import models.versions.GroundType;

import org.junit.Test;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;
import static util.ModelTestUtils.*;

public class EdgeTest {
  @Test
  public void serializesToJSON() throws Exception {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

    final Edge edge = new Edge(1, "test", "testKey", 2, 3, tagsMap);

    final String expected = convertFromClassToString(convertFromStringToClass(readFromFile
        ("test/resources/fixtures/models/edge.json"), Edge.class));
    assertThat(convertFromClassToString(edge)).isEqualTo(expected);
  }

  @Test
  public void deserializesFromJSON() throws Exception {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

    final Edge edge = new Edge(1, "test", "testKey", 2, 3, tagsMap);
    assertEquals(convertFromStringToClass(readFromFile("test/resources/fixtures/models/edge.json"),
        Edge.class), edge);
  }

  @Test
  public void testEdgeNotEquals() throws Exception {
    Edge truth = new Edge(1, "name", "sourceKey", 2, 3, new HashMap<>());
    assertFalse(truth.equals("notEdge"));

    Edge differentId = new Edge(2, "name", "sourceKey", 2, 3, new HashMap<>());
    assertFalse(truth.equals(differentId));

    Edge differentName = new Edge(1, "notName", "sourceKey", 2, 3, new HashMap<>());
    assertFalse(truth.equals(differentName));

    Edge differentKey = new Edge(1, "name", "notSourceKey", 2, 3, new HashMap<>());
    assertFalse(truth.equals(differentKey));

    Edge differentFromId =  new Edge(1, "name", "sourceKey", 4, 3, new HashMap<>());
    assertFalse(truth.equals(differentFromId));

    Edge differentToId =  new Edge(1, "name", "sourceKey", 2, 4, new HashMap<>());
    assertFalse(truth.equals(differentToId));

    Map<String, Tag> tags = new HashMap<>();
    tags.put("test", new Tag(1, "test", 1L, GroundType.LONG));
    Edge differentTags = new Edge(1, "name", "sourceKey", 2, 3, tags);
    assertFalse(truth.equals(differentTags));
  }
}
