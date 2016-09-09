package edu.berkeley.ground.api.models.gremlin;

import org.junit.Test;

import edu.berkeley.ground.api.GremlinTest;
import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.exceptions.GroundException;
import static org.junit.Assert.*;

public class GremlinNodeFactoryTest extends GremlinTest {

    public GremlinNodeFactoryTest() throws GroundException {
        super();
    }

    @Test
    public void testNodeCreation() {
        try {
            String testName = "test";
            GremlinNodeFactory edgeFactory = (GremlinNodeFactory) super.factories.getNodeFactory();
            edgeFactory.create(testName);

            Node edge = edgeFactory.retrieveFromDatabase(testName);

            assertEquals(testName, edge.getName());
            assertEquals("Nodes." + testName, edge.getId());
        } catch (GroundException ge) {
            fail(ge.getMessage());
        }
    }
}
