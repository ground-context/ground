package edu.berkeley.ground.common.model.core;

import static org.junit.Assert.assertFalse;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.model.version.GroundType;
import edu.berkeley.ground.common.model.version.Tag;
import org.junit.Test;

public class TagTest {

  @Test
  public void testTagNotEquals() throws Exception {
    Tag truth = new Tag(1, "test", 1L, GroundType.LONG);
    assertFalse(truth.equals("notTag"));

    Tag differentKey = new Tag(1, "notTest", 1L, GroundType.LONG);
    assertFalse(truth.equals(differentKey));

    Tag differentValue = new Tag(1, "test", "test2", GroundType.STRING);
    assertFalse(truth.equals(differentValue));
  }

  @Test(expected = GroundException.class)
  public void testTagMismatchTypeAndValue() throws GroundException {
    new Tag(1, "test", "test2", GroundType.LONG);
  }
}
