package edu.berkeley.ground.util;

import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;


/**
 * Sanity check for the test environment class
 */
public class TestEnvTest {

  @Test
  public void shouldLoadTestConfiguration() {
    assertThat(TestEnv.config.getBoolean("config.sanity.check")).isTrue();
  }

}
