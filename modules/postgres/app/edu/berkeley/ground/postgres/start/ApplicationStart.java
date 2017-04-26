package edu.berkeley.ground.postgres.start;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import javax.inject.Inject;
import javax.inject.Singleton;

import play.Logger;
import play.api.Configuration;
import play.inject.ApplicationLifecycle;

/**
 * This class demonstrates how to run code when the application starts and stops. It starts a timer
 * when the application starts. When the application stops it prints out how long the application
 * was running for.
 *
 * <p>This class is registered for Guice dependency injection in the {@link Module} class. We want
 * the class to start when the application starts, so it is registered as an "eager singleton". See
 * the code in the {@link Module} class to see how this happens.
 *
 * <p>This class needs to run code when the server stops. It uses the application's {@link
 * ApplicationLifecycle} to register a stop hook.
 */
@Singleton
public class ApplicationStart {

  private final Instant start;

  @Inject
  public ApplicationStart(
      Clock clock, ApplicationLifecycle appLifecycle, final Configuration configuration) {
    start = clock.instant();
    Logger.info("Ground Postgres: Starting application at " + start);

    Logger.info(
        "Ground Query will Cache for ---> {}",
        configuration.underlying().getString("ground.cache.expire.secs"));
    System.setProperty(
        "ground.cache.expire.secs",
        configuration.underlying().getString("ground.cache.expire.secs"));

    appLifecycle.addStopHook(
        () -> {
          Instant stop = clock.instant();
          Long runningTime = stop.getEpochSecond() - start.getEpochSecond();
          Logger.info(
              "Ground Postgres: Stopping application at "
                  + clock.instant()
                  + " after "
                  + runningTime
                  + "s.");
          return CompletableFuture.completedFuture(null);
        });
  }
}
