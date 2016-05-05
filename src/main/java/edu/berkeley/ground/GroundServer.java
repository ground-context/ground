package edu.berkeley.ground;

import edu.berkeley.ground.api.models.*;
import edu.berkeley.ground.api.models.postgres.PostgresEdgeFactory;
import edu.berkeley.ground.api.usage.LineageEdgeFactory;
import edu.berkeley.ground.api.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.resources.*;
import edu.berkeley.ground.util.PostgresFactories;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

// TODO: Clean up imports
// TODO: Check Logger classes
// TODO: Check formatting in factories
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

        EdgeFactory edgeFactory;
        EdgeVersionFactory edgeVersionFactory;
        GraphFactory graphFactory;
        GraphVersionFactory graphVersionFactory;
        LineageEdgeFactory lineageEdgeFactory;
        LineageEdgeVersionFactory lineageEdgeVersionFactory;
        NodeFactory nodeFactory;
        NodeVersionFactory nodeVersionFactory;
        StructureFactory structureFactory;
        StructureVersionFactory structureVersionFactory;


        switch (configuration.getDbType()) {
            case "postgres":
                dbClient = new PostgresClient(configuration.getDbHost(), configuration.getDbPort(), configuration.getDbName(), configuration.getDbUser(), configuration.getDbPassword());
                edgeFactory = PostgresFactories.getEdgeFactory();
                edgeVersionFactory = PostgresFactories.getEdgeVersionFactory();
                graphFactory = PostgresFactories.getGraphFactory();
                graphVersionFactory = PostgresFactories.getGraphVersionFactory();
                lineageEdgeFactory = PostgresFactories.getLineageEdgeFactory();
                lineageEdgeVersionFactory = PostgresFactories.getLineageEdgeVersionFactory();
                nodeFactory = PostgresFactories.getNodeFactory();
                nodeVersionFactory = PostgresFactories.getNodeVersionFactory();
                structureFactory = PostgresFactories.getStructureFactory();
                structureVersionFactory = PostgresFactories.getStructureVersionFactory();
                break;

            default: throw new RuntimeException("FATAL: Unrecognized database type (" + configuration.getDbType() + ").");
        }

        final EdgesResource edgesResource = new EdgesResource(dbClient, edgeFactory, edgeVersionFactory);
        final GraphsResource graphsResource = new GraphsResource(dbClient, graphFactory, graphVersionFactory);
        final LineageEdgesResource lineageEdgesResource = new LineageEdgesResource(dbClient, lineageEdgeFactory, lineageEdgeVersionFactory);
        final NodesResource nodesResource = new NodesResource(dbClient, nodeFactory, nodeVersionFactory);
        final StructuresResource structuresResource = new StructuresResource(dbClient, structureFactory, structureVersionFactory);

        environment.jersey().register(edgesResource);
        environment.jersey().register(graphsResource);
        environment.jersey().register(lineageEdgesResource);
        environment.jersey().register(nodesResource);
        environment.jersey().register(structuresResource);
    }
}