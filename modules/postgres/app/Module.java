import com.google.inject.AbstractModule;

import java.time.Clock;

import edu.berkeley.ground.postgres.start.ApplicationStart;

public class Module extends AbstractModule {

  @Override
  public void configure() {
    bind(Clock.class).toInstance(Clock.systemDefaultZone());
    bind(ApplicationStart.class).asEagerSingleton();
  }
}
