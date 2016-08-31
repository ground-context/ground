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

        NodeVersion nodeVersion = nodesResource.createNodeVersion(ModelCreateUtils.getNodeVersion("id", new HashMap<>(), null, null, new HashMap<>(), node.getId()), new ArrayList<>());
        assertThat(nodeVersion.getNodeId()).isEqualTo(node.getId());

        assertThat(nodeVersion.getParameters()).isEmpty();
        assertThat(nodeVersion.getReference()).isNull();
        assertThat(nodeVersion.getStructureVersionId()).isNull();
        assertThat(nodeVersion.getTags()).isEmpty();
    }

    @Test
    public void testReferences() throws GroundException {
        Node node = nodesResource.createNode("test");

        String reference = "http://www.google.com";

        Map<String, String> parameters = new HashMap<>();
        parameters.put("http", "GET");

        NodeVersion nodeVersion = nodesResource.createNodeVersion(ModelCreateUtils.getNodeVersion("id", new HashMap<>(), null, reference, parameters, node.getId()), new ArrayList<>());
        assertThat(nodeVersion.getNodeId()).isEqualTo(node.getId());

        assertThat(nodeVersion.getReference()).isNotNull();
        assertThat(nodeVersion.getParameters()).isNotEmpty();

        assertThat(nodeVersion.getReference()).isEqualTo("http://www.google.com");
        assertThat(nodeVersion.getParameters().size()).isEqualTo(1);
        assertThat(nodeVersion.getParameters().keySet()).contains("http");
        assertThat(nodeVersion.getParameters().get("http")).isEqualTo("GET");

    }

    @Test
    public void nodeWithMultipleParents() throws GroundException {
        Node node = nodesResource.createNode("test");

        NodeVersion nodeVersion = nodesResource.createNodeVersion(ModelCreateUtils.getNodeVersion("id", new HashMap<>(), null, null, new HashMap<>(), node.getId()), new ArrayList<>());

        List<String> parents = new ArrayList<>();
        parents.add(nodeVersion.getId());
        NodeVersion nodeVersionChild = nodesResource.createNodeVersion(ModelCreateUtils.getNodeVersion("id", new HashMap<>(), null, null, new HashMap<>(), node.getId()), parents);
        NodeVersion nodeVersionOtherChild = nodesResource.createNodeVersion(ModelCreateUtils.getNodeVersion("id", new HashMap<>(), null, null, new HashMap<>(), node.getId()), parents);

        parents.clear();
        parents.add(nodeVersionChild.getId());
        parents.add(nodeVersionOtherChild.getId());
        nodesResource.createNodeVersion(ModelCreateUtils.getNodeVersion("id", new HashMap<>(), null, null, new HashMap<>(), node.getId()), parents);
    }

    @Test
    public void getAdjacentNodes() throws GroundException {
        Node source = nodesResource.createNode("source");
        Node first = nodesResource.createNode("first");
        Node second = nodesResource.createNode("second");

        NodeVersion sourceVersion = nodesResource.createNodeVersion(ModelCreateUtils.getNodeVersion("id", new HashMap<>(), null, null, new HashMap<>(), source.getId()), new ArrayList<>());
        NodeVersion firstVersion = nodesResource.createNodeVersion(ModelCreateUtils.getNodeVersion("id", new HashMap<>(), null, null, new HashMap<>(), first.getId()), new ArrayList<>());
        NodeVersion secondVersion = nodesResource.createNodeVersion(ModelCreateUtils.getNodeVersion("id", new HashMap<>(), null, null, new HashMap<>(), second.getId()), new ArrayList<>());

        Edge firstEdge = edgesResource.createEdge("firstConnection");
        Edge secondEdge = edgesResource.createEdge("secondConnection");

        edgesResource.createEdgeVersion(ModelCreateUtils.getEdgeVersion("id", firstEdge.getId(), sourceVersion.getId(), firstVersion.getId()), new ArrayList<>());
        edgesResource.createEdgeVersion(ModelCreateUtils.getEdgeVersion("id", secondEdge.getId(), sourceVersion.getId(), secondVersion.getId()), new ArrayList<>());

        List<String> result = nodesResource.adjacentNodes(sourceVersion.getId(), "Connection");

        assertThat(result.contains(firstVersion.getId())).isTrue();
        assertThat(result.contains(secondVersion.getId())).isTrue();
    }
}
