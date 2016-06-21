package edu.berkeley.ground.api.versions.titan;

import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanVertex;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.api.versions.Version;
import edu.berkeley.ground.api.versions.VersionSuccessor;
import edu.berkeley.ground.api.versions.VersionSuccessorFactory;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.TitanClient.TitanConnection;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;

import java.util.ArrayList;
import java.util.List;

public class TitanVersionSuccessorFactory extends VersionSuccessorFactory {
    public <T extends Version> VersionSuccessor<T> create(GroundDBConnection connectionPointer, String fromId, String toId) throws GroundException {
        TitanConnection connection = (TitanConnection) connectionPointer;

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("id", Type.STRING, fromId));

        TitanVertex source = connection.getVertex(predicates);
        predicates.clear();

        predicates.add(new DbDataContainer("id", Type.STRING, toId));
        TitanVertex destination = connection.getVertex(predicates);

        String dbId = IdGenerator.generateId(fromId + toId);
        List<DbDataContainer> insertions = new ArrayList<>();
        insertions.add(new DbDataContainer("successor_id", Type.STRING, dbId));
        connection.addEdge("VersionSuccessor", source, destination, insertions);

        return VersionSuccessorFactory.construct(dbId, toId, fromId);
    }

    public <T extends Version> VersionSuccessor<T> retrieveFromDatabase(GroundDBConnection connectionPointer, String dbId) throws GroundException {
        TitanConnection connection = (TitanConnection) connectionPointer;

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("successor_id", Type.STRING, dbId));

        TitanEdge edge = connection.getEdge(predicates);

        return VersionSuccessorFactory.construct(dbId, (String) edge.outVertex().property("id").value(), (String) edge.inVertex().property("id").value());
    }
}
