package edu.berkeley.ground.api.models.postgres;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.berkeley.ground.api.PostgresTest;
import edu.berkeley.ground.api.models.NodeVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;
import static org.junit.Assert.*;

public class PostgresNodeVersionFactoryTest extends PostgresTest {

    public PostgresNodeVersionFactoryTest() throws GroundException {
        super();
    }

    @Test
    public void testNodeVersionCreation() throws GroundException {
        String nodeName = "testNode";
        String nodeId = super.factories.getNodeFactory().create(nodeName).getId();

        String structureName = "testStructure";
        String structureId =  super.factories.getStructureFactory().create(structureName).getId();

        Map<String, GroundType> structureVersionAttributes = new HashMap<>();
        structureVersionAttributes.put("intfield", GroundType.INTEGER);
        structureVersionAttributes.put("boolfield", GroundType.BOOLEAN);
        structureVersionAttributes.put("strfield", GroundType.STRING);

        String structureVersionId = super.factories.getStructureVersionFactory().create(
                structureId, structureVersionAttributes, new ArrayList<>()).getId();

        Map<String, Tag> tags = new HashMap<>();
        tags.put("intfield", new Tag(null, "intfield", 1, GroundType.INTEGER));
        tags.put("strfield", new Tag(null, "strfield", "1", GroundType.STRING));
        tags.put("boolfield", new Tag(null, "boolfield", true, GroundType.BOOLEAN));

        String testReference = "http://www.google.com";
        Map<String, String> parameters = new HashMap<>();
        parameters.put("http", "GET");

        String nodeVersionId = super.factories.getNodeVersionFactory().create(tags,
                structureVersionId, testReference, parameters, nodeId, new ArrayList<>()).getId();

        NodeVersion retrieved = super.factories.getNodeVersionFactory().retrieveFromDatabase(nodeVersionId);

        assertEquals(nodeId, retrieved.getNodeId());
        assertEquals(structureVersionId, retrieved.getStructureVersionId());
        assertEquals(testReference, retrieved.getReference());

        assertEquals(parameters.size(), retrieved.getParameters().size());
        assertEquals(tags.size(), retrieved.getTags().size());

        Map<String, String> retrievedParameters = retrieved.getParameters();
        Map<String, Tag> retrievedTags = retrieved.getTags();

        for (String key : parameters.keySet()) {
            assert(retrievedParameters).containsKey(key);
            assertEquals(parameters.get(key), retrievedParameters.get(key));
        }

        for (String key : tags.keySet()) {
            assert(retrievedTags).containsKey(key);
            assertEquals(tags.get(key), retrievedTags.get(key));
        }

        List<String> leaves = super.factories.getNodeFactory().getLeaves(nodeName);

        assertTrue(leaves.contains(nodeVersionId));
        assertTrue(1 == leaves.size());
    }
}
