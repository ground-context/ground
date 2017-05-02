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

package models.usage;

import models.models.Tag;
import models.versions.GroundType;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;
import static util.ModelTestUtils.*;

public class LineageEdgeTest {

  @Test
  public void serializesToJSON() throws Exception {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

    LineageEdge lineageEdge = new LineageEdge(1, "test", "testKey", tagsMap);

    final String expected = convertFromClassToString(convertFromStringToClass(readFromFile
        ("test/resources/fixtures/usage/lineage_edge.json"), LineageEdge.class));
    assertThat(convertFromClassToString(lineageEdge)).isEqualTo(expected);
  }

  @Test
  public void deserializesFromJSON() throws Exception {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

    LineageEdge lineageEdge = new LineageEdge(1, "test", "testKey", tagsMap);
    assertEquals(convertFromStringToClass(
        readFromFile("test/resources/fixtures/usage/lineage_edge.json"),
        LineageEdge.class), lineageEdge);
  }

  @Test
  public void testLineageEdgeNotEquals() throws Exception {
    LineageEdge truth = new LineageEdge(1, "name", "sourceKey", new HashMap<>());
    assertFalse(truth.equals("notLineageEdge"));

    LineageEdge differentId = new LineageEdge(2, "name", "sourceKey", new HashMap<>());
    assertFalse(truth.equals(differentId));

    LineageEdge differentName = new LineageEdge(1, "notName", "sourceKey", new HashMap<>());
    assertFalse(truth.equals(differentName));

    LineageEdge differentKey = new LineageEdge(1, "name", "notSourceKey", new HashMap<>());
    assertFalse(truth.equals(differentKey));

    Map<String, Tag> tags = new HashMap<>();
    tags.put("test", new Tag(1, "test", 1L, GroundType.LONG));
    LineageEdge differentTags = new LineageEdge(1, "name", "sourceKey", tags);
    assertFalse(truth.equals(differentTags));
  }
}
