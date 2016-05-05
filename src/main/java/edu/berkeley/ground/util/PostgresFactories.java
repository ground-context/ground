package edu.berkeley.ground.util;

import edu.berkeley.ground.api.models.*;
import edu.berkeley.ground.api.models.postgres.*;
import edu.berkeley.ground.api.usage.LineageEdgeFactory;
import edu.berkeley.ground.api.usage.LineageEdgeVersion;
import edu.berkeley.ground.api.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.api.usage.postgres.PostgresLineageEdgeFactory;
import edu.berkeley.ground.api.usage.postgres.PostgresLineageEdgeVersionFactory;
import edu.berkeley.ground.api.versions.postgres.PostgresItemFactory;
import edu.berkeley.ground.api.versions.postgres.PostgresVersionFactory;
import edu.berkeley.ground.api.versions.postgres.PostgresVersionHistoryDAGFactory;
import edu.berkeley.ground.api.versions.postgres.PostgresVersionSuccessorFactory;

public class PostgresFactories {
    private static PostgresVersionFactory versionFactory = new PostgresVersionFactory();
    private static PostgresVersionSuccessorFactory versionSuccessorFactory = new PostgresVersionSuccessorFactory();
    private static PostgresVersionHistoryDAGFactory versionHistoryDAGFactory = new PostgresVersionHistoryDAGFactory(versionSuccessorFactory);
    private static PostgresItemFactory itemFactory = new PostgresItemFactory(versionHistoryDAGFactory);

    private static PostgresStructureFactory structureFactory = new PostgresStructureFactory(itemFactory);
    private static PostgresStructureVersionFactory structureVersionFactory = new PostgresStructureVersionFactory(structureFactory, versionFactory);
    private static PostgresTagFactory tagFactory = new PostgresTagFactory();
    private static PostgresRichVersionFactory richVersionFactory = new PostgresRichVersionFactory(versionFactory, structureVersionFactory, tagFactory);
    private static PostgresEdgeFactory edgeFactory = new PostgresEdgeFactory(itemFactory);
    private static PostgresEdgeVersionFactory edgeVersionFactory = new PostgresEdgeVersionFactory(edgeFactory, richVersionFactory);
    private static PostgresGraphFactory graphFactory = new PostgresGraphFactory(itemFactory);
    private static PostgresGraphVersionFactory graphVersionFactory = new PostgresGraphVersionFactory(graphFactory, richVersionFactory);
    private static PostgresNodeFactory nodeFactory = new PostgresNodeFactory(itemFactory);
    private static PostgresNodeVersionFactory nodeVersionFactory = new PostgresNodeVersionFactory(nodeFactory, richVersionFactory);

    private static PostgresLineageEdgeFactory lineageEdgeFactory = new PostgresLineageEdgeFactory(itemFactory);
    private static PostgresLineageEdgeVersionFactory lineageEdgeVersionFactory = new PostgresLineageEdgeVersionFactory(lineageEdgeFactory, richVersionFactory);

    public static EdgeFactory getEdgeFactory() {
        return edgeFactory;
    }

    public static EdgeVersionFactory getEdgeVersionFactory() {
        return edgeVersionFactory;
    }

    public static GraphFactory getGraphFactory() {
        return graphFactory;
    }

    public static GraphVersionFactory getGraphVersionFactory() {
        return graphVersionFactory;
    }

    public static NodeFactory getNodeFactory() {
        return nodeFactory;
    }

    public static NodeVersionFactory getNodeVersionFactory() {
        return nodeVersionFactory;
    }

    public static LineageEdgeFactory getLineageEdgeFactory() {
        return lineageEdgeFactory;
    }

    public static LineageEdgeVersionFactory getLineageEdgeVersionFactory() {
        return lineageEdgeVersionFactory;
    }

    public static StructureFactory getStructureFactory() {
        return structureFactory;
    }

    public static StructureVersionFactory getStructureVersionFactory() {
        return structureVersionFactory;
    }
}
