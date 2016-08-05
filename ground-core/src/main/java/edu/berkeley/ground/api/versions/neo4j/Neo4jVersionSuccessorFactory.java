package edu.berkeley.ground.api.versions.neo4j;

import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.api.versions.Version;
import edu.berkeley.ground.api.versions.VersionSuccessor;
import edu.berkeley.ground.api.versions.VersionSuccessorFactory;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient.Neo4jConnection;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;
import org.neo4j.driver.v1.Record;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Neo4jVersionSuccessorFactory extends VersionSuccessorFactory {
    public <T extends Version> VersionSuccessor<T> create(GroundDBConnection connectionPointer, String fromId, String toId) throws GroundException {
        Neo4jConnection connection = (Neo4jConnection) connectionPointer;
        String dbId = IdGenerator.generateId(fromId + toId);

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("id", Type.STRING, dbId));

        connection.addEdge("VersionSuccessor", fromId, toId, predicates);

        return VersionSuccessorFactory.construct(dbId, toId, fromId);
    }


    public <T extends Version> VersionSuccessor<T> retrieveFromDatabase(GroundDBConnection connectionPointer, String dbId) throws GroundException {
        Neo4jConnection connection = (Neo4jConnection) connectionPointer;
        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("id", Type.STRING, dbId));

        Record result = connection.getEdge("VersionSuccessor", predicates);

        return this.constructFromRecord(result);
    }

    protected <T extends Version> VersionSuccessor<T> constructFromRecord(Record r) {
        return VersionSuccessorFactory.construct(r.get("id").toString(), r.get("fromId").toString(), r.get("toId").toString());
    }
}
