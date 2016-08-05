package edu.berkeley.ground.api.versions.neo4j;

import edu.berkeley.ground.api.versions.Version;
import edu.berkeley.ground.api.versions.VersionHistoryDAG;
import edu.berkeley.ground.api.versions.VersionHistoryDAGFactory;
import edu.berkeley.ground.api.versions.VersionSuccessor;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.Neo4jClient.Neo4jConnection;
import edu.berkeley.ground.exceptions.GroundException;
import org.neo4j.driver.v1.Record;

import java.util.ArrayList;
import java.util.List;

public class Neo4jVersionHistoryDAGFactory extends VersionHistoryDAGFactory {
    private Neo4jVersionSuccessorFactory versionSuccessorFactory;

    public Neo4jVersionHistoryDAGFactory(Neo4jVersionSuccessorFactory versionSuccessorFactory) {
        this.versionSuccessorFactory = versionSuccessorFactory;
    }

    public <T extends Version> VersionHistoryDAG<T> create(String itemId) throws GroundException {
        return construct(itemId);
    }

    public <T extends Version> VersionHistoryDAG<T> retrieveFromDatabase(GroundDBConnection connectionPointer, String itemId) throws GroundException {
        Neo4jConnection connection = (Neo4jConnection) connectionPointer;
        List<Record> result = connection.getDescendantEdgesByLabel(itemId, "VersionSuccessor");

        if (result.isEmpty()) {
            throw new GroundException("No results found for query");
        }

        List<VersionSuccessor<T>> edges = new ArrayList<>();

        for (Record record : result) {
            edges.add(this.versionSuccessorFactory.constructFromRecord(record));
        }

        return construct(itemId, edges);
    }

    public void addEdge(GroundDBConnection connection, VersionHistoryDAG dag, String parentId, String childId, String itemId) throws GroundException {
        VersionSuccessor successor = this.versionSuccessorFactory.create(connection, parentId, childId);

        dag.addEdge(parentId, childId, successor.getId());
    }
}
