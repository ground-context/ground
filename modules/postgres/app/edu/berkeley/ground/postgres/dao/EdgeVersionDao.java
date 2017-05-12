package edu.berkeley.ground.postgres.dao;

import com.fasterxml.jackson.databind.JsonNode;
import edu.berkeley.ground.common.exception.GroundException;
import edu.berkeley.ground.common.factory.core.EdgeVersionFactory;
import edu.berkeley.ground.common.model.core.EdgeVersion;
import edu.berkeley.ground.common.utils.IdGenerator;
import edu.berkeley.ground.postgres.utils.PostgresUtils;

import java.util.ArrayList;
import java.util.List;

import play.db.Database;
import play.libs.Json;


public class EdgeVersionDao extends RichVersionDao<EdgeVersion> implements EdgeVersionFactory {

    public final void create(final Database dbSource, final EdgeVersion edgeVersion, final IdGenerator idGenerator, List<Long> parentIds)
        throws GroundException {
        final List<String> sqlList = new ArrayList<>();
        final long uniqueId = idGenerator.generateVersionId();
        //final Map<String, Tag> tags = edgeVersion.getTags();
        EdgeVersion newEdgeVersion = new EdgeVersion(uniqueId, edgeVersion.getTags(), edgeVersion.getStructureVersionId(),
        	edgeVersion.getReference(), edgeVersion.getParameters(), edgeVersion.getEdgeId(), edgeVersion.getFromNodeVersionStartId(),
            edgeVersion.getFromNodeVersionEndId(), edgeVersion.getToNodeVersionStartId(), edgeVersion.getToNodeVersionEndId());
        new EdgeDao().update(edgeVersion.getEdgeId(), edgeVersion.getId(), parentIds);
        try {
        	sqlList.addAll(super.createSqlList(dbSource, newEdgeVersion));
        	sqlList.add(
        		String.format(
        			"insert into edge_version (id, edge_id, from_node_start_id, from_node_end_id, to_node_start_id, to_node_end_id) values (%d, %d, %d, %d, %d, %d)",
        			uniqueId, edgeVersion.getEdgeId(), edgeVersion.getFromNodeVersionStartId(), edgeVersion.getFromNodeVersionEndId(), edgeVersion.getToNodeVersionStartId(), edgeVersion.getToNodeVersionEndId()));
            PostgresUtils.executeSqlList(dbSource, sqlList);
        }
        catch (Exception e) {
            throw new GroundException(e);
        }
    }

    @Override
    public EdgeVersion retrieveFromDatabase(Database dbSource, long id) throws GroundException {
        String sql = String.format("select * from edge_version where id=%d", id);
        JsonNode json = Json.parse(PostgresUtils.executeQueryToJson(dbSource, sql));
        return Json.fromJson(json, EdgeVersion.class);
    }
}
