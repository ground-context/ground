package edu.berkeley.ground.common.util;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class IdGeneratorTest {

  @Test
  public void testMultipleMachineGenerator() {
    IdGenerator generator = new IdGenerator(3, 4, false);
    long id = generator.generateItemId();

    long expected = (3L << 62) | 1L;

    assertEquals(expected, id);
  }
}
