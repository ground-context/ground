package edu.berkeley.ground.common.model.versions;

import static org.junit.Assert.assertEquals;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.version.GroundType;
import org.junit.Test;

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
  public void testParse() throws GroundException {
    assertEquals(GroundType.STRING.parse(null), null);
    assertEquals(GroundType.STRING.parse("hello"), "hello");
    assertEquals(GroundType.INTEGER.parse("1234"), 1234);
    assertEquals(GroundType.LONG.parse("54321"), 54321L);
    assertEquals(GroundType.BOOLEAN.parse("true"), true);
    assertEquals(GroundType.BOOLEAN.parse("false"), false);
  }
}
