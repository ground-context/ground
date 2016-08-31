/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.resources;

import edu.berkeley.ground.GroundResourceTest;
import edu.berkeley.ground.api.models.*;
import edu.berkeley.ground.exceptions.GroundException;
import io.dropwizard.jersey.params.NonEmptyStringParam;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class EdgesResourceTest extends GroundResourceTest {
    @Test
    public void createEdgeVersion() throws GroundException {
        Node node = nodesResource.createNode("test");
        NodeVersion nodeVersion = nodesResource.createNodeVersion(ModelCreateUtils.getNodeVersion("id", new HashMap<>(), null, null, new HashMap<>(), node.getId()), new ArrayList<>());

        Edge edge = edgesResource.createEdge("test");
        assertThat(edge.getName()).isEqualTo("test");
        EdgeVersion edgeVersion = edgesResource.createEdgeVersion(ModelCreateUtils.getEdgeVersion("id", edge.getId(), nodeVersion.getId(), nodeVersion.getId()), new ArrayList<>());

        assertThat(edgeVersion.getEdgeId()).isEqualTo(edge.getId());
        assertThat(edgeVersion.getFromId()).isEqualTo(nodeVersion.getId());
        assertThat(edgeVersion.getToId()).isEqualTo(nodeVersion.getId());

        assertThat(edgeVersion.getParameters()).isEmpty();
        assertThat(edgeVersion.getReference()).isNull();
        assertThat(edgeVersion.getStructureVersionId()).isNull();
        assertThat(edgeVersion.getTags()).isEmpty();
    }
}
