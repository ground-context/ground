package edu.berkeley.ground;

import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.resources.NodesResource;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class GroundServer extends Application<GroundServerConfiguration> {
    public static void main(String [] args) throws Exception {
        new GroundServer().run(args);
    }

    @Override
    public String getName() {
        return "ground-server";
    }

    @Override
    public void initialize(Bootstrap<GroundServerConfiguration> bootstrap){
        // nothing
    }

    @Override
    public void run(GroundServerConfiguration configuration, Environment environment) {
        DBClient dbClient;
        switch (configuration.getDbType()) {
            case "postgres":
                dbClient = new PostgresClient(configuration.getDbHost(), configuration.getDbPort(), configuration.getDbName(), configuration.getDbUser(), configuration.getDbPassword());
                break;

            default: throw new RuntimeException("FATAL: Unrecognized database type (" + configuration.getDbType() + ").");
        }

        final NodesResource nodesResource = new NodesResource(dbClient);
        environment.jersey().register(nodesResource);
    }
}