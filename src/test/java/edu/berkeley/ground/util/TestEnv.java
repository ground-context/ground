package edu.berkeley.ground.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class TestEnv {

  private static final String TEST_CONFIG = "test.conf";

  public static final Config config = ConfigFactory.load(TEST_CONFIG);

  private TestEnv() {
    // NOOP Constructor - static class
  }

}
