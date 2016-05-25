package edu.berkeley.ground.resources;

import edu.berkeley.ground.GroundResourceTest;
import edu.berkeley.ground.api.models.*;
import edu.berkeley.ground.exceptions.GroundException;
import io.dropwizard.jersey.params.NonEmptyStringParam;
import org.junit.Test;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class EdgesResourceTest extends GroundResourceTest {
    @Test
    public void createEdgeVersion() throws GroundException {
        Node node = nodesResource.createNode("test");
        NodeVersion nodeVersion = nodesResource.createNodeVersion(ModelCreateUtils.getNodeVersion("id", Optional.<Map<String, Tag>>empty(), Optional.<String>empty(), Optional.<String>empty(), Optional.<Map<String, String>>empty(), node.getId()), new NonEmptyStringParam(null));

        Edge edge = edgesResource.createEdge("test");
        assertThat(edge.getName()).isEqualTo("test");
        EdgeVersion edgeVersion = edgesResource.createEdgeVersion(ModelCreateUtils.getEdgeVersion("id", edge.getId(), nodeVersion.getId(), nodeVersion.getId()), new NonEmptyStringParam(null));

        assertThat(edgeVersion.getEdgeId()).isEqualTo(edge.getId());
        assertThat(edgeVersion.getFromId()).isEqualTo(nodeVersion.getId());
        assertThat(edgeVersion.getToId()).isEqualTo(nodeVersion.getId());

        assertThat(edgeVersion.getParameters()).isEmpty();
        assertThat(edgeVersion.getReference()).isEmpty();
        assertThat(edgeVersion.getStructureVersionId()).isEmpty();
        assertThat(edgeVersion.getTags()).isEmpty();
    }
}
