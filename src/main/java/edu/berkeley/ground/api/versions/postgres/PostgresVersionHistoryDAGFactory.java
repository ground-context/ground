package edu.berkeley.ground.api.versions.postgres;

import edu.berkeley.ground.api.versions.*;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.QueryResults;
import edu.berkeley.ground.exceptions.GroundDBException;
import edu.berkeley.ground.exceptions.GroundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PostgresVersionHistoryDAGFactory extends VersionHistoryDAGFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(VersionHistoryDAG.class);

    private PostgresVersionSuccessorFactory versionSuccessorFactory;

    public PostgresVersionHistoryDAGFactory(PostgresVersionSuccessorFactory versionSuccessorFactory) {
        this.versionSuccessorFactory = versionSuccessorFactory;
    }

    public <T extends Version> VersionHistoryDAG<T> create(String itemId) throws GroundException {
        return construct(itemId);
    }

    public <T extends Version> VersionHistoryDAG<T> retrieveFromDatabase(GroundDBConnection connection, String itemId) throws GroundException {
        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("item_id", Type.STRING, itemId));

        QueryResults resultSet = connection.equalitySelect("VersionHistoryDAGs", DBClient.SELECT_STAR, predicates);

        List<VersionSuccessor<T>> edges = new ArrayList<>();
        do {
            edges.add(this.versionSuccessorFactory.retrieveFromDatabase(connection, resultSet.getInt(2)));
        } while (resultSet.next());

        return VersionHistoryDAGFactory.construct(itemId, edges);
    }

    public void addEdge(GroundDBConnection connection, VersionHistoryDAG dag, String parentId, String childId, String itemId) throws GroundException {
        VersionSuccessor successor = this.versionSuccessorFactory.create(connection, parentId, childId);

        List<DbDataContainer> insertions = new ArrayList<>();
        insertions.add(new DbDataContainer("item_id", Type.STRING, itemId));
        insertions.add(new DbDataContainer("successor_id", Type.INTEGER, (int) successor.getId()));

        connection.insert("VersionHistoryDAGs", insertions);

        dag.addEdge(parentId, childId, successor.getId());
    }
}
