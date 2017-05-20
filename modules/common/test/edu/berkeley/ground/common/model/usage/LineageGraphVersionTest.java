/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.common.model.usage;

import static edu.berkeley.ground.common.util.ModelTestUtils.convertFromClassToString;
import static edu.berkeley.ground.common.util.ModelTestUtils.convertFromStringToClass;
import static edu.berkeley.ground.common.util.ModelTestUtils.readFromFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import edu.berkeley.ground.common.model.version.GroundType;
import edu.berkeley.ground.common.model.version.Tag;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class LineageGraphVersionTest {

  @Test
  public void serializesToJSON() throws Exception {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

    Map<String, String> parametersMap = new HashMap<>();
    parametersMap.put("http", "GET");

    List<Long> lineagedgeVersionIds = new ArrayList<>();
    lineagedgeVersionIds.add(123L);
    lineagedgeVersionIds.add(456L);

    LineageGraphVersion lineageGraphVersion = new LineageGraphVersion(1, tagsMap, -1,
      "http://www.google.com", parametersMap, 1, lineagedgeVersionIds);

    final String expected = convertFromClassToString(convertFromStringToClass(readFromFile
      ("test/resources/fixtures/usage/lineage_graph_version.json"), LineageGraphVersion.class));
    assertEquals(convertFromClassToString(lineageGraphVersion), expected);
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

    assertEquals(convertFromStringToClass
      (readFromFile("test/resources/fixtures/usage/lineage_graph_version.json"),
        LineageGraphVersion.class), lineageGraphVersion);
  }

  @Test
  public void testLineageGraphVersionNotEquals() throws Exception {
    LineageGraphVersion truth = new LineageGraphVersion(1, new HashMap<>(), 2,
      "http://www.google.com", new HashMap<>(), 3, new ArrayList<>());
    assertFalse(truth.equals("notGraphVersion"));

    LineageGraphVersion differentId = new LineageGraphVersion(2, new HashMap<>(), 2,
      "http://www.google.com", new HashMap<>(), 3, new ArrayList<>());
    assertFalse(truth.equals(differentId));

    Map<String, Tag> tags = new HashMap<>();
    tags.put("test", new Tag(1, "test", 1L, GroundType.LONG));
    LineageGraphVersion differentTags = new LineageGraphVersion(1, tags, 2, "http://www.google.com",
      new HashMap<>(), 3, new ArrayList<>());
    assertFalse(truth.equals(differentTags));

    LineageGraphVersion differenLineageGraphVersionId = new LineageGraphVersion(1, new HashMap<>(),
      10,
      "http://www.google.com", new HashMap<>(), 3, new ArrayList<>());
    assertFalse(truth.equals(differenLineageGraphVersionId));

    LineageGraphVersion differentReference = new LineageGraphVersion(1, new HashMap<>(), 2,
      "http://www.fb.com", new HashMap<>(), 3, new ArrayList<>());
    assertFalse(truth.equals(differentReference));

    Map<String, String> parameters = new HashMap<>();
    parameters.put("test", "param");
    LineageGraphVersion differentParameters = new LineageGraphVersion(1, new HashMap<>(), 2,
      "http://www.google.com", parameters, 3, new ArrayList<>());
    assertFalse(truth.equals(differentParameters));

    LineageGraphVersion differentGraphId = new LineageGraphVersion(1, new HashMap<>(), 2,
      "http://www.google.com", new HashMap<>(), 10, new ArrayList<>());
    assertFalse(truth.equals(differentGraphId));

    List<Long> ids = new ArrayList<>();
    ids.add(10L);
    LineageGraphVersion differentEdgeVersionIds = new LineageGraphVersion(1, new HashMap<>(), 2,
      "http://www.google.com", new HashMap<>(), 3, ids);
    assertFalse(truth.equals(differentEdgeVersionIds));
  }
}
