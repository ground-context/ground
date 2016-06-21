package edu.berkeley.ground.util;

import edu.berkeley.ground.api.models.*;
import edu.berkeley.ground.api.models.titan.*;
import edu.berkeley.ground.api.usage.LineageEdgeFactory;
import edu.berkeley.ground.api.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.api.usage.titan.TitanLineageEdgeFactory;
import edu.berkeley.ground.api.usage.titan.TitanLineageEdgeVersionFactory;
import edu.berkeley.ground.api.versions.titan.TitanItemFactory;
import edu.berkeley.ground.api.versions.titan.TitanVersionHistoryDAGFactory;
import edu.berkeley.ground.api.versions.titan.TitanVersionSuccessorFactory;
import edu.berkeley.ground.db.TitanClient;

public class TitanFactories {
    private TitanStructureFactory structureFactory;
    private TitanStructureVersionFactory structureVersionFactory;
    private TitanEdgeFactory edgeFactory;
    private TitanEdgeVersionFactory edgeVersionFactory;
    private TitanGraphFactory graphFactory;
    private TitanGraphVersionFactory graphVersionFactory;
    private TitanNodeFactory nodeFactory;
    private TitanNodeVersionFactory nodeVersionFactory;

    private TitanLineageEdgeFactory lineageEdgeFactory;
    private TitanLineageEdgeVersionFactory lineageEdgeVersionFactory;

    public TitanFactories(TitanClient cassandraClient, ElasticSearchClient elasticSearchClient) {
        TitanVersionSuccessorFactory versionSuccessorFactory = new TitanVersionSuccessorFactory();
        TitanVersionHistoryDAGFactory versionHistoryDAGFactory = new TitanVersionHistoryDAGFactory(versionSuccessorFactory);
        TitanItemFactory itemFactory = new TitanItemFactory(versionHistoryDAGFactory);

        this.structureFactory = new TitanStructureFactory(itemFactory, cassandraClient);
        this.structureVersionFactory = new TitanStructureVersionFactory(this.structureFactory, cassandraClient);
        TitanTagFactory tagFactory = new TitanTagFactory();
        TitanRichVersionFactory richVersionFactory = new TitanRichVersionFactory(structureVersionFactory, tagFactory, elasticSearchClient);
        this.edgeFactory = new TitanEdgeFactory(itemFactory, cassandraClient);
        this.edgeVersionFactory = new TitanEdgeVersionFactory(this.edgeFactory, richVersionFactory, cassandraClient);
        this.graphFactory = new TitanGraphFactory(itemFactory, cassandraClient);
        this.graphVersionFactory = new TitanGraphVersionFactory(this.graphFactory, richVersionFactory, cassandraClient);
        this.nodeFactory = new TitanNodeFactory(itemFactory, cassandraClient);
        this.nodeVersionFactory = new TitanNodeVersionFactory(this.nodeFactory, richVersionFactory, cassandraClient);

        this.lineageEdgeFactory = new TitanLineageEdgeFactory(itemFactory, cassandraClient);
        this.lineageEdgeVersionFactory = new TitanLineageEdgeVersionFactory(this.lineageEdgeFactory, richVersionFactory, cassandraClient);
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
