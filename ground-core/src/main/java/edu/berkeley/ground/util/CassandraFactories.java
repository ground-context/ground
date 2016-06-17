package edu.berkeley.ground.util;

import edu.berkeley.ground.api.models.*;
import edu.berkeley.ground.api.models.cassandra.*;
import edu.berkeley.ground.api.usage.LineageEdgeFactory;
import edu.berkeley.ground.api.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.api.usage.cassandra.CassandraLineageEdgeFactory;
import edu.berkeley.ground.api.usage.cassandra.CassandraLineageEdgeVersionFactory;
import edu.berkeley.ground.api.versions.cassandra.CassandraItemFactory;
import edu.berkeley.ground.api.versions.cassandra.CassandraVersionFactory;
import edu.berkeley.ground.api.versions.cassandra.CassandraVersionHistoryDAGFactory;
import edu.berkeley.ground.api.versions.cassandra.CassandraVersionSuccessorFactory;
import edu.berkeley.ground.db.CassandraClient;

public class CassandraFactories {
    private CassandraStructureFactory structureFactory;
    private CassandraStructureVersionFactory structureVersionFactory;
    private CassandraEdgeFactory edgeFactory;
    private CassandraEdgeVersionFactory edgeVersionFactory;
    private CassandraGraphFactory graphFactory;
    private CassandraGraphVersionFactory graphVersionFactory;
    private CassandraNodeFactory nodeFactory;
    private CassandraNodeVersionFactory nodeVersionFactory;

    private CassandraLineageEdgeFactory lineageEdgeFactory;
    private CassandraLineageEdgeVersionFactory lineageEdgeVersionFactory;

    public CassandraFactories(CassandraClient cassandraClient, ElasticSearchClient elasticSearchClient) {
        CassandraVersionFactory versionFactory = new CassandraVersionFactory();
        CassandraVersionSuccessorFactory versionSuccessorFactory = new CassandraVersionSuccessorFactory();
        CassandraVersionHistoryDAGFactory versionHistoryDAGFactory = new CassandraVersionHistoryDAGFactory(versionSuccessorFactory);
        CassandraItemFactory itemFactory = new CassandraItemFactory(versionHistoryDAGFactory);

        this.structureFactory = new CassandraStructureFactory(itemFactory, cassandraClient);
        this.structureVersionFactory = new CassandraStructureVersionFactory(this.structureFactory, versionFactory, cassandraClient);
        CassandraTagFactory tagFactory = new CassandraTagFactory();
        CassandraRichVersionFactory richVersionFactory = new CassandraRichVersionFactory(versionFactory, structureVersionFactory, tagFactory, elasticSearchClient);
        this.edgeFactory = new CassandraEdgeFactory(itemFactory, cassandraClient);
        this.edgeVersionFactory = new CassandraEdgeVersionFactory(this.edgeFactory, richVersionFactory, cassandraClient);
        this.graphFactory = new CassandraGraphFactory(itemFactory, cassandraClient);
        this.graphVersionFactory = new CassandraGraphVersionFactory(this.graphFactory, richVersionFactory, cassandraClient);
        this.nodeFactory = new CassandraNodeFactory(itemFactory, cassandraClient);
        this.nodeVersionFactory = new CassandraNodeVersionFactory(this.nodeFactory, richVersionFactory, cassandraClient);

        this.lineageEdgeFactory = new CassandraLineageEdgeFactory(itemFactory, cassandraClient);
        this.lineageEdgeVersionFactory = new CassandraLineageEdgeVersionFactory(this.lineageEdgeFactory, richVersionFactory, cassandraClient);
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
