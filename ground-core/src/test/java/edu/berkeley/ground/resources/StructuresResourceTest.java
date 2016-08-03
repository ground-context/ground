package edu.berkeley.ground.resources;

import edu.berkeley.ground.GroundResourceTest;
import edu.berkeley.ground.api.models.*;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;
import io.dropwizard.jersey.params.NonEmptyStringParam;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class StructuresResourceTest extends GroundResourceTest {
    @Test
    public void createStructure() throws GroundException {
        Structure structure = structuresResource.createStructure("test");
        Map<String, GroundType> attributes = new HashMap<>();
        attributes.put("test", GroundType.STRING);

        StructureVersion structureVersion = structuresResource.createStructureVersion(ModelCreateUtils.getStructureVersion("id", structure.getId(), attributes), new NonEmptyStringParam(null));

        assertThat(structureVersion.getStructureId()).isEqualTo(structure.getId());
        assertThat(structureVersion.getAttributes().size()).isEqualTo(1);
        assertThat(structureVersion.getAttributes()).containsKey("test");
        assertThat(structureVersion.getAttributes().get("test")).isEqualTo(GroundType.STRING);
    }

    @Test
    public void checkStructureAttributesArePresent() throws GroundException {
        Structure structure = structuresResource.createStructure("test");
        Map<String, GroundType> attributes = new HashMap<>();
        attributes.put("test", GroundType.STRING);

        StructureVersion structureVersion = structuresResource.createStructureVersion(ModelCreateUtils.getStructureVersion("id", structure.getId(), attributes), new NonEmptyStringParam(null));

        Map<String, Tag> tagsMap = new HashMap<>();
        tagsMap.put("test", new Tag(null, "test", Optional.of("a"), Optional.of(GroundType.STRING)));

        Node node = nodesResource.createNode("test");
        NodeVersion nodeVersion = nodesResource.createNodeVersion(ModelCreateUtils.getNodeVersion("id", Optional.of(tagsMap), Optional.of(structureVersion.getId()), Optional.<String>empty(), Optional.<Map<String, String>>empty(), node.getId()), new NonEmptyStringParam(null));

        assertThat(nodeVersion.getStructureVersionId()).isPresent();
        assertThat(nodeVersion.getStructureVersionId().get()).isEqualTo(structureVersion.getId());
        assertThat(nodeVersion.getTags()).isPresent();
        assertThat(nodeVersion.getTags().get()).containsKey("test");
        assertThat(nodeVersion.getTags().get().get("test").getValue()).isPresent();
        assertThat(nodeVersion.getTags().get().get("test").getValue().get()).isEqualTo("a");

    }
}
