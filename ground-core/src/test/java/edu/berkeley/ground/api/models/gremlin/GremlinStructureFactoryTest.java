package edu.berkeley.ground.api.models.gremlin;

import org.junit.Test;

import edu.berkeley.ground.api.GremlinTest;
import edu.berkeley.ground.api.models.Structure;
import edu.berkeley.ground.exceptions.GroundException;
import static org.junit.Assert.*;

public class GremlinStructureFactoryTest extends GremlinTest {

    public GremlinStructureFactoryTest() throws GroundException {
        super();
    }

    @Test
    public void testStructureCreation() {
        try {
            String testName = "test";
            GremlinStructureFactory edgeFactory = (GremlinStructureFactory) super.factories.getStructureFactory();
            edgeFactory.create(testName);

            Structure edge = edgeFactory.retrieveFromDatabase(testName);

            assertEquals(testName, edge.getName());
            assertEquals("Structures." + testName, edge.getId());
        } catch (GroundException ge) {
            fail(ge.getMessage());
        }
    }
}
