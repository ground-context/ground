package edu.berkeley.ground.util;

import org.junit.Test;

import static org.junit.Assert.*;

public class IdGeneratorTest {

  @Test
  public void testMultipleMachineGenerator() {
    IdGenerator generator = new IdGenerator(3, 4, false);
    long id = generator.generateItemId();

    long expected = (3L << 62) | 1L;

    assertEquals(expected, id);
  }
}
