package edu.berkeley.ground.postgres.dao;

import java.util.ArrayList;
import java.util.List;
import java.sql.Connection;
import java.util.Map;

import edu.berkeley.ground.lib.exception.GroundException;
import edu.berkeley.ground.lib.model.core.EdgeVersion;
import edu.berkeley.ground.lib.model.core.EdgeVersionFactory;
import edu.berkeley.ground.postgres.utils.PostgresUtils;
import play.db.Database;
import play.db.ConnectionCallable;
import play.db.ConnectionRunnable;
import play.libs.Json;
import javax.sql.DataSource;
import com.fasterxml.jackson.databind.JsonNode;
import berkeley.edu.ground.lib.model.version.Tag;
import edu.berkeley.ground.postgres.utils.IdGenerator;
import edu.berkeley.ground.postgres.utils.PostgresUtils;

public class EdgeVersionDao extends RichVersionDao<EdgeVersion> implements EdgeVersionFactory {

	@Override
    public final void create(final Database dbSource, final EdgeVersion edgeVersion, final IdGenerator idGenerator) throws GroundException {
        final List<String> sqlList = new ArrayList<>();
        long uniqueId = idGenerator.generateItemId();
        final Map<String, Tag> tags = edgeVersion.getTags();
        EdgeVersion newEdgeVersion = new EdgeVersion(uniqueId, edgeVersion.getTags(), edgeVersion.getStructureVersionId(),
        	edgeVersion.getReference(), edgeVersion.getReferenceParameters(), edgeVersion.getEdgeId());
        try {
        	sqlList.addAll(super.createSqlList(dbSource, newEdgeVersion));
        	sqlList.add(
        		String.format(
        			"insert into edge_version (id, edge_id, from_node_start_id, from_node_end_id, to_node_start_id, to_node_end_id) values (%d, %d, %d, %d, %d, %d)", 
        			uniqueId, edgeVersion.getEdgeId(), edgeVersion.getFromNodeVersionStartId(), edgeVersion.getFromNodeVersionEndId(), edgeVersion.getToNodeVersionStartId(), edgeVersion.getToNodeVersionEndId()));
        }
        
        PostgresUtils.executeSqlList(dbSource, sqlList);
    }
}