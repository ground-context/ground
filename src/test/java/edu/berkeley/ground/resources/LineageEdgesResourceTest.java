package edu.berkeley.ground.resources;

import edu.berkeley.ground.GroundTest;
import edu.berkeley.ground.api.models.*;
import edu.berkeley.ground.api.usage.LineageEdge;
import edu.berkeley.ground.api.usage.LineageEdgeVersion;
import edu.berkeley.ground.api.usage.UsageCreateUtils;
import edu.berkeley.ground.exceptions.GroundException;
import io.dropwizard.jersey.params.NonEmptyStringParam;
import org.junit.Test;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class LineageEdgesResourceTest extends GroundTest {
    @Test
    public void createLineageEdge() throws GroundException {
        Node node = nodesResource.createNode("test");
        NodeVersion nodeVersion = nodesResource.createNodeVersion(ModelCreateUtils.getNodeVersion("id", Optional.<Map<String, Tag>>empty(), Optional.<String>empty(), node.getId()), new NonEmptyStringParam(null));

        Edge edge = edgesResource.createEdge("test");
        EdgeVersion edgeVersion = edgesResource.createEdgeVersion(ModelCreateUtils.getEdgeVersion("id", edge.getId(), nodeVersion.getId(), nodeVersion.getId()), new NonEmptyStringParam(null));

        LineageEdge lineageEdge = lineageEdgesResource.createLineageEdge("test");
        LineageEdgeVersion lineageEdgeVersion = lineageEdgesResource.createLineageEdgeVersion(UsageCreateUtils.getLineageEdgeVersion("id", lineageEdge.getId(), nodeVersion.getId(), edgeVersion.getId()), new NonEmptyStringParam(null));
        assertThat(lineageEdgeVersion.getLineageEdgeId()).isEqualTo(lineageEdge.getId());
        assertThat(lineageEdgeVersion.getFromId()).isEqualTo(nodeVersion.getId());
        assertThat(lineageEdgeVersion.getToId()).isEqualTo(edgeVersion.getId());

        assertThat(lineageEdgeVersion.getParameters()).isEmpty();
        assertThat(lineageEdgeVersion.getReference()).isEmpty();
        assertThat(lineageEdgeVersion.getStructureVersionId()).isEmpty();
        assertThat(lineageEdgeVersion.getTags()).isEmpty();
    }
}
