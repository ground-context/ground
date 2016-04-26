package edu.berkeley.ground.resources;

import edu.berkeley.ground.GroundTest;
import edu.berkeley.ground.api.models.ModelCreateUtils;
import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.NodeVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.exceptions.GroundException;
import io.dropwizard.jersey.params.NonEmptyStringParam;
import org.junit.Test;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class NodesResourceTest extends GroundTest {
    @Test
    public void createNodeVersion() throws GroundException {
        Node node = nodesResource.createNode("test");
        assertThat(node.getName()).isEqualTo("test");

        NodeVersion nodeVersion = nodesResource.createNodeVersion(ModelCreateUtils.getNodeVersion("id", Optional.<Map<String, Tag>>empty(), Optional.<String>empty(), node.getId()), new NonEmptyStringParam(null));
        assertThat(nodeVersion.getNodeId()).isEqualTo(node.getId());

        assertThat(nodeVersion.getParameters()).isEmpty();
        assertThat(nodeVersion.getReference()).isEmpty();
        assertThat(nodeVersion.getStructureVersionId()).isEmpty();
        assertThat(nodeVersion.getTags()).isEmpty();
    }
}
