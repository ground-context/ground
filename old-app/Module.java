import com.google.inject.AbstractModule;
import play.Configuration;
import play.Environment;
import util.CassandraFactories;
import util.FactoryGenerator;
import util.Neo4jFactories;
import util.PostgresFactories;

/**
 * This class is a Guice module that tells Guice how to bind several
 * different types. This Guice module is created when the Play
 * application starts.
 *
 * Play will automatically use any class called `Module` that is in
 * the root package. You can create modules in other locations by
 * adding `play.modules.enabled` settings to the `application.conf`
 * configuration file.
 */
public class Module extends AbstractModule {

  private final Configuration configuration;

  public Module(Environment environment, Configuration configuration) {
    this.configuration = configuration;
  }

  @Override
  public void configure() {
    Configuration dbConf = this.configuration.getConfig("db");
    switch (dbConf.getString("type")) {
      case "postgres":
        bind(FactoryGenerator.class).to(PostgresFactories.class);
        break;
      case "neo4j":
        bind(FactoryGenerator.class).to(Neo4jFactories.class);
        break;
      case "cassandra":
        bind(FactoryGenerator.class).to(CassandraFactories.class);
        break;
      default:
        throw new RuntimeException("Unexpected database type " + dbConf.getString("type"));
    }


  }
}
