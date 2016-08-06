package edu.berkeley.ground.resources;

import edu.berkeley.ground.GroundResourceTest;
import edu.berkeley.ground.api.models.ModelCreateUtils;
import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.NodeVersion;
import edu.berkeley.ground.api.models.Tag;
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
        NodeVersion finalChild = nodesResource.createNodeVersion(ModelCreateUtils.getNodeVersion("id", Optional.<Map<String, Tag>>empty(), Optional.<String>empty(), Optional.<String>empty(), Optional.<Map<String, String>>empty(), node.getId()), parents);
    }
}
