package edu.berkeley.ground.plugins.hive;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for the GroundStore.
 */
public class GroundStoreTest extends TestCase {
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public GroundStoreTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(GroundStoreTest.class);
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp() {
        assertTrue(true);
    }
}
