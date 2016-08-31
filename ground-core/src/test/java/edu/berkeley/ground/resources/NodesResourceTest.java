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

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class NodesResourceTest extends GroundResourceTest {
    @Test
    public void createNodeVersion() throws GroundException {
        Node node = nodesResource.createNode("test");
        assertThat(node.getName()).isEqualTo("test");

        NodeVersion nodeVersion = nodesResource.createNodeVersion(ModelCreateUtils.getNodeVersion("id", Optional.<Map<String, Tag>>empty(), Optional.<String>empty(), Optional.<String>empty(), Optional.<Map<String, String>>empty(), node.getId()), new ArrayList<>());
        assertThat(nodeVersion.getNodeId()).isEqualTo(node.getId());

        assertThat(nodeVersion.getParameters()).isEmpty();
        assertThat(nodeVersion.getReference()).isEmpty();
        assertThat(nodeVersion.getStructureVersionId()).isEmpty();
        assertThat(nodeVersion.getTags()).isEmpty();
    }

    @Test
    public void testReferences() throws GroundException {
        Node node = nodesResource.createNode("test");

        Optional<String> reference = Optional.of("http://www.google.com");

        Map<String, String> parametersMap = new HashMap<>();
        parametersMap.put("http", "GET");
        Optional<Map<String, String>> parameters = Optional.of(parametersMap);

        NodeVersion nodeVersion = nodesResource.createNodeVersion(ModelCreateUtils.getNodeVersion("id", Optional.<Map<String, Tag>>empty(), Optional.<String>empty(), reference, parameters, node.getId()), new ArrayList<>());
        assertThat(nodeVersion.getNodeId()).isEqualTo(node.getId());

        assertThat(nodeVersion.getReference()).isPresent();
        assertThat(nodeVersion.getParameters()).isPresent();

        assertThat(nodeVersion.getReference().get()).isEqualTo("http://www.google.com");
        assertThat(nodeVersion.getParameters().get().size()).isEqualTo(1);
        assertThat(nodeVersion.getParameters().get().keySet()).contains("http");
        assertThat(nodeVersion.getParameters().get().get("http")).isEqualTo("GET");

    }

    @Test
    public void nodeWithMultipleParents() throws GroundException {
        Node node = nodesResource.createNode("test");

        NodeVersion nodeVersion = nodesResource.createNodeVersion(ModelCreateUtils.getNodeVersion("id", Optional.<Map<String, Tag>>empty(), Optional.<String>empty(), Optional.<String>empty(), Optional.<Map<String, String>>empty(), node.getId()), new ArrayList<>());

        List<String> parents = new ArrayList<>();
        parents.add(nodeVersion.getId());
        NodeVersion nodeVersionChild = nodesResource.createNodeVersion(ModelCreateUtils.getNodeVersion("id", Optional.<Map<String, Tag>>empty(), Optional.<String>empty(), Optional.<String>empty(), Optional.<Map<String, String>>empty(), node.getId()), parents);
        NodeVersion nodeVersionOtherChild = nodesResource.createNodeVersion(ModelCreateUtils.getNodeVersion("id", Optional.<Map<String, Tag>>empty(), Optional.<String>empty(), Optional.<String>empty(), Optional.<Map<String, String>>empty(), node.getId()), parents);

        parents.clear();
        parents.add(nodeVersionChild.getId());
        parents.add(nodeVersionOtherChild.getId());
        nodesResource.createNodeVersion(ModelCreateUtils.getNodeVersion("id", Optional.<Map<String, Tag>>empty(), Optional.<String>empty(), Optional.<String>empty(), Optional.<Map<String, String>>empty(), node.getId()), parents);
    }

    @Test
    public void getAdjacentNodes() throws GroundException {
        Node source = nodesResource.createNode("source");
        Node first = nodesResource.createNode("first");
        Node second = nodesResource.createNode("second");

        NodeVersion sourceVersion = nodesResource.createNodeVersion(ModelCreateUtils.getNodeVersion("id", Optional.<Map<String, Tag>>empty(), Optional.<String>empty(), Optional.<String>empty(), Optional.<Map<String, String>>empty(), source.getId()), new ArrayList<>());
        NodeVersion firstVersion = nodesResource.createNodeVersion(ModelCreateUtils.getNodeVersion("id", Optional.<Map<String, Tag>>empty(), Optional.<String>empty(), Optional.<String>empty(), Optional.<Map<String, String>>empty(), first.getId()), new ArrayList<>());
        NodeVersion secondVersion = nodesResource.createNodeVersion(ModelCreateUtils.getNodeVersion("id", Optional.<Map<String, Tag>>empty(), Optional.<String>empty(), Optional.<String>empty(), Optional.<Map<String, String>>empty(), second.getId()), new ArrayList<>());

        Edge firstEdge = edgesResource.createEdge("firstConnection");
        Edge secondEdge = edgesResource.createEdge("secondConnection");

        edgesResource.createEdgeVersion(ModelCreateUtils.getEdgeVersion("id", firstEdge.getId(), sourceVersion.getId(), firstVersion.getId()), new ArrayList<>());
        edgesResource.createEdgeVersion(ModelCreateUtils.getEdgeVersion("id", secondEdge.getId(), sourceVersion.getId(), secondVersion.getId()), new ArrayList<>());

        List<String> result = nodesResource.adjacentNodes(sourceVersion.getId(), "Connection");

        assertThat(result.contains(firstVersion.getId())).isTrue();
        assertThat(result.contains(secondVersion.getId())).isTrue();
    }
}
