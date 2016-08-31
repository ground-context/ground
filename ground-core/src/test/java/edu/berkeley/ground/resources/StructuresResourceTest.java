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
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;
import io.dropwizard.jersey.params.NonEmptyStringParam;
import org.junit.Test;

import java.util.ArrayList;
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

        StructureVersion structureVersion = structuresResource.createStructureVersion(ModelCreateUtils.getStructureVersion("id", structure.getId(), attributes), new ArrayList<>());

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

        StructureVersion structureVersion = structuresResource.createStructureVersion(ModelCreateUtils.getStructureVersion("id", structure.getId(), attributes), new ArrayList<>());

        Map<String, Tag> tagsMap = new HashMap<>();
        tagsMap.put("test", new Tag(null, "test", "a", GroundType.STRING));

        Node node = nodesResource.createNode("test");
        NodeVersion nodeVersion = nodesResource.createNodeVersion(ModelCreateUtils.getNodeVersion("id", tagsMap, structureVersion.getId(), null, new HashMap<>(), node.getId()), new ArrayList<>());

        assertThat(nodeVersion.getStructureVersionId()).isNotNull();
        assertThat(nodeVersion.getStructureVersionId()).isEqualTo(structureVersion.getId());
        assertThat(nodeVersion.getTags()).isNotNull();
        assertThat(nodeVersion.getTags()).containsKey("test");
        assertThat(nodeVersion.getTags().get("test").getValue()).isEqualTo("a");
    }
}
