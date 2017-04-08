package edu.berkeley.ground.model.versions;

import org.junit.Test;

import edu.berkeley.ground.exceptions.GroundException;

import static org.junit.Assert.*;

public class GroundTypeTest {

  @Test
  public void testGetTypeFromString() throws GroundException {
    assertEquals(GroundType.BOOLEAN, GroundType.fromString("boolean"));
    assertEquals(GroundType.INTEGER, GroundType.fromString("integer"));
    assertEquals(GroundType.LONG, GroundType.fromString("long"));
    assertEquals(GroundType.STRING, GroundType.fromString("string"));
  }

  @Test(expected = GroundException.class)
  public void testGetBadType() throws GroundException {
    GroundType.fromString("notType");
  }

  @Test
  public void testStringToType() throws GroundException {
    assertEquals(null, GroundType.stringToType(null, GroundType.STRING));
    assertEquals("hello", GroundType.stringToType("hello", GroundType.STRING));
    assertEquals(1, GroundType.stringToType("1", GroundType.INTEGER));
    assertEquals(1L, GroundType.stringToType("1", GroundType.LONG));
    assertEquals(true, GroundType.stringToType("true", GroundType.BOOLEAN));
  }
}
