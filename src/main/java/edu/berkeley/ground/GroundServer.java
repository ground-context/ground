package edu.berkeley.ground;

import edu.berkeley.ground.api.models.*;
import edu.berkeley.ground.api.usage.LineageEdgeFactory;
import edu.berkeley.ground.api.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.db.TitanClient;
import edu.berkeley.ground.resources.*;
import edu.berkeley.ground.util.CassandraFactories;
import edu.berkeley.ground.util.PostgresFactories;
import edu.berkeley.ground.util.TitanFactories;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

// TODO: Clean up imports
// TODO: Check Logger classes at creation match actual classes
// TODO: Check formatting in factories
public class GroundServer extends Application<GroundServerConfiguration> {
    private EdgeFactory edgeFactory;
    private EdgeVersionFactory edgeVersionFactory;
    private GraphFactory graphFactory;
    private GraphVersionFactory graphVersionFactory;
    private LineageEdgeFactory lineageEdgeFactory;
    private LineageEdgeVersionFactory lineageEdgeVersionFactory;
    private NodeFactory nodeFactory;
    private NodeVersionFactory nodeVersionFactory;
    private StructureFactory structureFactory;
    private StructureVersionFactory structureVersionFactory;

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
        switch (configuration.getDbType()) {
            case "postgres":
                PostgresClient postgresClient = new PostgresClient(configuration.getDbHost(), configuration.getDbPort(), configuration.getDbName(), configuration.getDbUser(), configuration.getDbPassword());
                setPostgresFactories(postgresClient);
                break;

            case "cassandra":
                CassandraClient cassandraClient = new CassandraClient(configuration.getDbHost(), configuration.getDbPort(), configuration.getDbName(), configuration.getDbUser(), configuration.getDbPassword());
                setCassandraFactories(cassandraClient);
                break;

            case "titan":
                TitanClient titanClient = new TitanClient(false);
                setTitanFactories(titanClient);

            default: throw new RuntimeException("FATAL: Unrecognized database type (" + configuration.getDbType() + ").");
        }

        final EdgesResource edgesResource = new EdgesResource(edgeFactory, edgeVersionFactory);
        final GraphsResource graphsResource = new GraphsResource(graphFactory, graphVersionFactory);
        final LineageEdgesResource lineageEdgesResource = new LineageEdgesResource(lineageEdgeFactory, lineageEdgeVersionFactory);
        final NodesResource nodesResource = new NodesResource(nodeFactory, nodeVersionFactory);
        final StructuresResource structuresResource = new StructuresResource(structureFactory, structureVersionFactory);

        environment.jersey().register(edgesResource);
        environment.jersey().register(graphsResource);
        environment.jersey().register(lineageEdgesResource);
        environment.jersey().register(nodesResource);
        environment.jersey().register(structuresResource);
    }

    private void setPostgresFactories(PostgresClient postgresClient) {
        PostgresFactories factoryGenerator = new PostgresFactories(postgresClient);

        edgeFactory = factoryGenerator.getEdgeFactory();
        edgeVersionFactory = factoryGenerator.getEdgeVersionFactory();
        graphFactory = factoryGenerator.getGraphFactory();
        graphVersionFactory = factoryGenerator.getGraphVersionFactory();
        lineageEdgeFactory = factoryGenerator.getLineageEdgeFactory();
        lineageEdgeVersionFactory = factoryGenerator.getLineageEdgeVersionFactory();
        nodeFactory = factoryGenerator.getNodeFactory();
        nodeVersionFactory = factoryGenerator.getNodeVersionFactory();
        structureFactory = factoryGenerator.getStructureFactory();
        structureVersionFactory = factoryGenerator.getStructureVersionFactory();
    }

    private void setCassandraFactories(CassandraClient cassandraClient) {
        CassandraFactories factoryGenerator = new CassandraFactories(cassandraClient);

        edgeFactory = factoryGenerator.getEdgeFactory();
        edgeVersionFactory = factoryGenerator.getEdgeVersionFactory();
        graphFactory = factoryGenerator.getGraphFactory();
        graphVersionFactory = factoryGenerator.getGraphVersionFactory();
        lineageEdgeFactory = factoryGenerator.getLineageEdgeFactory();
        lineageEdgeVersionFactory = factoryGenerator.getLineageEdgeVersionFactory();
        nodeFactory = factoryGenerator.getNodeFactory();
        nodeVersionFactory = factoryGenerator.getNodeVersionFactory();
        structureFactory = factoryGenerator.getStructureFactory();
        structureVersionFactory = factoryGenerator.getStructureVersionFactory();
    }

    private void setTitanFactories(TitanClient titanClient) {
        TitanFactories factoryGenerator = new TitanFactories(titanClient);

        edgeFactory = factoryGenerator.getEdgeFactory();
        edgeVersionFactory = factoryGenerator.getEdgeVersionFactory();
        graphFactory = factoryGenerator.getGraphFactory();
        graphVersionFactory = factoryGenerator.getGraphVersionFactory();
        lineageEdgeFactory = factoryGenerator.getLineageEdgeFactory();
        lineageEdgeVersionFactory = factoryGenerator.getLineageEdgeVersionFactory();
        nodeFactory = factoryGenerator.getNodeFactory();
        nodeVersionFactory = factoryGenerator.getNodeVersionFactory();
        structureFactory = factoryGenerator.getStructureFactory();
        structureVersionFactory = factoryGenerator.getStructureVersionFactory();
    }
}