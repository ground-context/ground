package edu.berkeley.ground.util;

import edu.berkeley.ground.api.models.*;
import edu.berkeley.ground.api.models.postgres.*;
import edu.berkeley.ground.api.usage.LineageEdgeFactory;
import edu.berkeley.ground.api.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.api.usage.postgres.PostgresLineageEdgeFactory;
import edu.berkeley.ground.api.usage.postgres.PostgresLineageEdgeVersionFactory;
import edu.berkeley.ground.api.versions.postgres.PostgresItemFactory;
import edu.berkeley.ground.api.versions.postgres.PostgresVersionFactory;
import edu.berkeley.ground.api.versions.postgres.PostgresVersionHistoryDAGFactory;
import edu.berkeley.ground.api.versions.postgres.PostgresVersionSuccessorFactory;
import edu.berkeley.ground.db.PostgresClient;

public class PostgresFactories {
    private PostgresStructureFactory structureFactory;
    private PostgresStructureVersionFactory structureVersionFactory;
    private PostgresEdgeFactory edgeFactory;
    private PostgresEdgeVersionFactory edgeVersionFactory;
    private PostgresGraphFactory graphFactory;
    private PostgresGraphVersionFactory graphVersionFactory;
    private PostgresNodeFactory nodeFactory;
    private PostgresNodeVersionFactory nodeVersionFactory;

    private PostgresLineageEdgeFactory lineageEdgeFactory;
    private PostgresLineageEdgeVersionFactory lineageEdgeVersionFactory;

    public PostgresFactories(PostgresClient postgresClient) {
        PostgresVersionFactory versionFactory = new PostgresVersionFactory();
        PostgresVersionSuccessorFactory versionSuccessorFactory = new PostgresVersionSuccessorFactory();
        PostgresVersionHistoryDAGFactory versionHistoryDAGFactory = new PostgresVersionHistoryDAGFactory(versionSuccessorFactory);
        PostgresItemFactory itemFactory = new PostgresItemFactory(versionHistoryDAGFactory);

        this.structureFactory = new PostgresStructureFactory(itemFactory, postgresClient);
        this.structureVersionFactory = new PostgresStructureVersionFactory(this.structureFactory, versionFactory, postgresClient);
        PostgresTagFactory tagFactory = new PostgresTagFactory();
        PostgresRichVersionFactory richVersionFactory = new PostgresRichVersionFactory(versionFactory, structureVersionFactory, tagFactory);
        this.edgeFactory = new PostgresEdgeFactory(itemFactory, postgresClient);
        this.edgeVersionFactory = new PostgresEdgeVersionFactory(this.edgeFactory, richVersionFactory, postgresClient);
        this.graphFactory = new PostgresGraphFactory(itemFactory, postgresClient);
        this.graphVersionFactory = new PostgresGraphVersionFactory(this.graphFactory, richVersionFactory, postgresClient);
        this.nodeFactory = new PostgresNodeFactory(itemFactory, postgresClient);
        this.nodeVersionFactory = new PostgresNodeVersionFactory(this.nodeFactory, richVersionFactory, postgresClient);

        this.lineageEdgeFactory = new PostgresLineageEdgeFactory(itemFactory, postgresClient);
        this.lineageEdgeVersionFactory = new PostgresLineageEdgeVersionFactory(this.lineageEdgeFactory, richVersionFactory, postgresClient);
    }

    public EdgeFactory getEdgeFactory() {
        return edgeFactory;
    }

    public EdgeVersionFactory getEdgeVersionFactory() {
        return edgeVersionFactory;
    }

    public GraphFactory getGraphFactory() {
        return graphFactory;
    }

    public GraphVersionFactory getGraphVersionFactory() {
        return graphVersionFactory;
    }

    public NodeFactory getNodeFactory() {
        return nodeFactory;
    }

    public NodeVersionFactory getNodeVersionFactory() {
        return nodeVersionFactory;
    }

    public LineageEdgeFactory getLineageEdgeFactory() {
        return lineageEdgeFactory;
    }

    public LineageEdgeVersionFactory getLineageEdgeVersionFactory() {
        return lineageEdgeVersionFactory;
    }

    public StructureFactory getStructureFactory() {
        return structureFactory;
    }

    public StructureVersionFactory getStructureVersionFactory() {
        return structureVersionFactory;
    }
}
