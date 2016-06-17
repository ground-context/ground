package edu.berkeley.ground.api.versions.titan;

import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanVertex;
import edu.berkeley.ground.api.versions.*;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.TitanClient.TitanConnection;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.ArrayList;
import java.util.List;

public class TitanVersionHistoryDAGFactory extends VersionHistoryDAGFactory {
    private TitanVersionSuccessorFactory versionSuccessorFactory;

    public TitanVersionHistoryDAGFactory(TitanVersionSuccessorFactory versionSuccessorFactory) {
        this.versionSuccessorFactory = versionSuccessorFactory;
    }

    public <T extends Version> VersionHistoryDAG<T> create(String itemId) throws GroundException {
        return construct(itemId);
    }

    public <T extends Version> VersionHistoryDAG<T> retrieveFromDatabase(GroundDBConnection connectionPointer, String itemId) throws GroundException {
        TitanConnection connection = (TitanConnection) connectionPointer;

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("id", Type.STRING, itemId));

        TitanVertex itemVertex = connection.getVertex(predicates);
        List<TitanEdge> titanEdges = connection.getDescendantEdgesWithLabel(itemVertex, "VersionSuccessor");

        List<VersionSuccessor<T>> edges = new ArrayList<>();

        for (TitanEdge titanEdge : titanEdges) {
            edges.add(this.versionSuccessorFactory.retrieveFromDatabase(connection, (String) titanEdge.property("successor_id").value()));
        }

        return VersionHistoryDAGFactory.construct(itemId, edges);
    }

    public void addEdge(GroundDBConnection connectionPointer, VersionHistoryDAG dag, String parentId, String childId, String itemId) throws GroundException {
        VersionSuccessor successor = this.versionSuccessorFactory.create(connectionPointer, parentId, childId);

        dag.addEdge(parentId, childId, successor.getId());
    }
}
