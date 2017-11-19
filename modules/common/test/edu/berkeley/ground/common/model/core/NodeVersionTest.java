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

package edu.berkeley.ground.common.model.core;

import static edu.berkeley.ground.common.util.ModelTestUtils.convertFromClassToString;
import static edu.berkeley.ground.common.util.ModelTestUtils.convertFromStringToClass;
import static edu.berkeley.ground.common.util.ModelTestUtils.readFromFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import edu.berkeley.ground.common.model.version.GroundType;
import edu.berkeley.ground.common.model.version.Tag;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import org.junit.Test;

public class NodeVersionTest {

  @Test
  public void serializesToJSON() throws Exception {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

    Map<String, String> parametersMap = new HashMap<>();
    parametersMap.put("http", "GET");

    NodeVersion nodeVersion = new NodeVersion(1, tagsMap, -1, "http://www.google.com",
      parametersMap, 1);

    final String expected = convertFromClassToString(convertFromStringToClass(readFromFile
      ("test/resources/fixtures/core/node_version.json"), NodeVersion.class));
    assertEquals(convertFromClassToString(nodeVersion), expected);
  }

  @Test
  public void deserializesFromJSON() throws Exception {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag", new Tag(1, "testtag", "tag", GroundType.STRING));

    Map<String, String> parametersMap = new HashMap<>();
    parametersMap.put("http", "GET");

    NodeVersion nodeVersion = new NodeVersion(1, tagsMap, -1, "http://www.google.com",
      parametersMap, 1);

    assertEquals(convertFromStringToClass(
      readFromFile("test/resources/fixtures/core/node_version.json"),
      NodeVersion.class), nodeVersion);
  }

  @Test
  public void testNodeVersionNotEquals() throws Exception {
    NodeVersion truth = new NodeVersion(1, new HashMap<>(), 2, "http://www.google.com",
      new HashMap<>(), 3);
    assertFalse(truth.equals("notNodeVersion"));

    NodeVersion differentId = new NodeVersion(2, new HashMap<>(), 2, "http://www.google.com",
      new HashMap<>(), 3);
    assertFalse(truth.equals(differentId));

    Map<String, Tag> tags = new HashMap<>();
    tags.put("test", new Tag(1, "test", 1L, GroundType.LONG));
    NodeVersion differentTags = new NodeVersion(1, tags, 2, "http://www.google.com",
      new HashMap<>(), 3);
    assertFalse(truth.equals(differentTags));

    NodeVersion differenStructureVersionId = new NodeVersion(1, new HashMap<>(), 10,
      "http://www.google.com", new HashMap<>(), 3);
    assertFalse(truth.equals(differenStructureVersionId));

    NodeVersion differentReference = new NodeVersion(1, new HashMap<>(), 2, "http://www.fb.com",
      new HashMap<>(), 3);
    assertFalse(truth.equals(differentReference));

    Map<String, String> parameters = new HashMap<>();
    parameters.put("test", "param");
    NodeVersion differentParameters = new NodeVersion(1, new HashMap<>(), 2,
      "http://www.google.com", parameters, 3);
    assertFalse(truth.equals(differentParameters));

    NodeVersion differentNodeId = new NodeVersion(1, new HashMap<>(), 2, "http://www.google.com",
      new HashMap<>(), 10);
    assertFalse(truth.equals(differentNodeId));
  }

  @Test
  public void testNodeVersionTags() throws Exception {
    Map<String, Tag> tagsMap = new HashMap<>();
    tagsMap.put("testtag1", new Tag(1, "testtag1", "tag", GroundType.STRING));
    tagsMap.put("testtag2", new Tag(2, "testtag2", 2, GroundType.INTEGER));
    tagsMap.put("testtag3", new Tag(3, "testtag3", true, GroundType.BOOLEAN));
    tagsMap.put("testtag4", new Tag(4, "testtag4", 9999999999999, GroundType.LONG));

    ArrayList array = new ArrayList<Integer>(Arrays.asList(1, 2, 3, 4));
    LinkedHashMap json = new LinkedHashMap<String, Object>();
    json.put("ting", "skrr");
    json.put("maths", 3);

    Map<String, Tag> tagsMapEqual = new HashMap<>();
    tagsMapEqual.put("testtag1", new Tag(1, "testtag1", 45.6, GroundType.DOUBLE));
    tagsMapEqual.put("testtag2", new Tag(2, "testtag2", array, GroundType.ARRAY));
    tagsMapEqual.put("testtag3", new Tag(3, "testtag3", null, GroundType.NULL));
    tagsMapEqual.put("testtag4", new Tag(4, "testtag4", json, GroundType.JSON));

    Map<String, String> parametersMap = new HashMap<>();
    parametersMap.put("http", "POST");

    Map<String, String> parametersMapEqual = new HashMap<>();
    parametersMapEqual.put("http", "POST");

    NodeVersion nodeVersion = new NodeVersion(1, tagsMap, 2, "http://www.google.com", parametersMap, 3);
    
    NodeVersion nodeVersionEqual = new NodeVersion(1, tagsMapEqual, 2, "http://www.google.com", parametersMapEqual, 3);

    assertFalse(nodeVersion.equals(nodeVersionEqual));

  }
}
