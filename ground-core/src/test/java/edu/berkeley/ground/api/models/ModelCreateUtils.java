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

import edu.berkeley.ground.api.versions.GroundType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ModelCreateUtils {
  public static NodeVersion getNodeVersion(String id, Map<String, Tag> tags, String structureVersionId, String reference, Map<String, String> parameters, String nodeId) {
    return new NodeVersion(id, tags, structureVersionId, reference, parameters, nodeId);
  }

  public static EdgeVersion getEdgeVersion(String id, String edgeId, String fromId, String toId) {
    return new EdgeVersion(id, new HashMap<>(), null, null, new HashMap<>(), edgeId, fromId, toId);
  }

  public static GraphVersion getGraphVersion(String id, String graphId, List<String> edgeVersionIds) {
    return new GraphVersion(id, new HashMap<>(), null, null, new HashMap<>(), graphId, edgeVersionIds);
  }

  public static StructureVersion getStructureVersion(String id, String structureId, Map<String, GroundType> attributes) {
    return new StructureVersion(id, structureId, attributes);
  }
}
