package edu.berkeley.ground.util;

import org.junit.Test;

import static org.junit.Assert.*;

public class IdGeneratorTest {

  @Test
  public void testMultipleMachineGenerator() {
    IdGenerator generator = new IdGenerator(1, 2, false);
    long id = generator.generateItemId();

    long expected = (1L << 63) | 1L;

    assertEquals(expected, id);
  }
}
