package edu.berkeley.ground.cassandra.start;

import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.cassandra.util.CassandraDatabase;
import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import javax.inject.Inject;
import javax.inject.Singleton;
import play.Logger;
import play.api.Configuration;
// import play.db.Database;
import play.inject.ApplicationLifecycle;

@Singleton
public class ApplicationStart {

  private final Instant start;

  @Inject
  public ApplicationStart(Clock clock, ApplicationLifecycle appLifecycle, final Configuration configuration, final CassandraDatabase dbSource)
    throws GroundException {

    this.start = clock.instant();
    Logger.info("Ground Cassandra: Starting application at " + this.start);

    Logger.info("Queries will Cache for {} seconds.", configuration.underlying().getString("ground.cache.expire.secs"));
    System.setProperty("ground.cache.expire.secs", configuration.underlying().getString("ground.cache.expire.secs"));

    appLifecycle.addStopHook(
      () -> {
        Instant stop = clock.instant();
        Long runningTime = stop.getEpochSecond() - this.start.getEpochSecond();
        Logger.info("Ground Cassandra: Stopping application at " + clock.instant() + " after " + runningTime + "s.");
        return CompletableFuture.completedFuture(null);
      });
  }
}
