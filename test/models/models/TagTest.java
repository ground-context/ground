package models.models;

import exceptions.GroundException;
import org.junit.Test;

import models.versions.GroundType;

import static org.junit.Assert.*;

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
