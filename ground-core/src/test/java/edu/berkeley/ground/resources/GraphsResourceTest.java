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
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class GraphsResourceTest extends GroundResourceTest {
    @Test
    public void createGraphVersion() throws GroundException {
        Node node = nodesResource.createNode("test");
        NodeVersion nodeVersion = nodesResource.createNodeVersion(ModelCreateUtils.getNodeVersion("id", Optional.<Map<String, Tag>>empty(), Optional.<String>empty(), Optional.<String>empty(), Optional.<Map<String, String>>empty(), node.getId()), new ArrayList<>());

        Edge edge = edgesResource.createEdge("test");
        EdgeVersion edgeVersion = edgesResource.createEdgeVersion(ModelCreateUtils.getEdgeVersion("id", edge.getId(), nodeVersion.getId(), nodeVersion.getId()), new ArrayList<>());

        Graph graph = graphsResource.createGraph("test");
        assertThat(graph.getName()).isEqualTo("test");
        List<String> edgeVersionIds = new ArrayList<>();
        edgeVersionIds.add(edgeVersion.getId());

        GraphVersion graphVersion = graphsResource.createGraphVersion(ModelCreateUtils.getGraphVersion("id", graph.getId(), edgeVersionIds), new ArrayList<>());
        assertThat(graphVersion.getGraphId()).isEqualTo(graph.getId());
        assertThat(graphVersion.getEdgeVersionIds().size()).isEqualTo(1);
        assertThat(graphVersion.getEdgeVersionIds().get(0)).isEqualTo(edgeVersion.getId());

        assertThat(graphVersion.getParameters()).isEmpty();
        assertThat(graphVersion.getReference()).isEmpty();
        assertThat(graphVersion.getStructureVersionId()).isEmpty();
        assertThat(graphVersion.getTags()).isEmpty();
    }
}
