package edu.berkeley.ground.plugins.hive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.common.util.HiveStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import edu.berkeley.ground.api.models.Edge;
import edu.berkeley.ground.api.models.EdgeFactory;
import edu.berkeley.ground.api.models.EdgeVersion;
import edu.berkeley.ground.api.models.EdgeVersionFactory;
import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.NodeVersion;
import edu.berkeley.ground.api.models.Structure;
import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.plugins.hive.GroundStore.EntityState;

public class GTable {
    static final private Logger LOG = LoggerFactory.getLogger(GTable.class.getName());

    static final String DATABASE_NODE = "_TABLE";

    static final String DATABASE_TABLE_EDGE = "_TABLE_PARTITION";

    private static final List<String> EMPTY_PARENT_LIST = new ArrayList<String>();
    
    private GroundReadWrite ground = null;

    public GTable(GroundReadWrite ground) {
        this.ground = ground;
    }

    public Node getNode(String tableName) throws GroundException {
        try {
            LOG.debug("Fetching table node: " + tableName);
            return ground.getNodeFactory().retrieveFromDatabase(tableName);
        } catch (GroundException ge1) {
            LOG.debug("Not found - Creating table node: " + tableName);

            Node node = ground.getNodeFactory().create(tableName);
            Structure nodeStruct = ground.getStructureFactory().create(node.getName());

            return node;
        }
    }

    public Structure getNodeStructure(String tableName) throws GroundException {
        try {
            Node node = this.getNode(tableName);
            return ground.getStructureFactory().retrieveFromDatabase(tableName);
        } catch (GroundException e) {
            LOG.error("Unable to fetch table node structure");
            throw e;
        }
    }

    public Edge getEdge(String partitionName) throws GroundException {
        try {
            LOG.debug("Fetching table partition edge: " + partitionName);
            return ground.getEdgeFactory().retrieveFromDatabase(partitionName);
        } catch (GroundException ge1) {
            LOG.debug("Not found - Creating table partition edge: " + partitionName);

            Edge edge = ground.getEdgeFactory().create(partitionName);
            Structure edgeStruct = ground.getStructureFactory().create(partitionName);
            return edge;
        }
    }

    public Structure getEdgeStructure(String partitionName) throws GroundException {
        try {
            Edge edge = getEdge(partitionName);
            return ground.getStructureFactory().retrieveFromDatabase(partitionName);
        } catch (GroundException e) {
            LOG.error("Unable to fetch table partition edge structure");
            throw e;
        }
    }

    public Table fromJSON(String json) {
        Gson gson = new Gson();
        return (Table) gson.fromJson(json, Table.class);
    }

    public String toJSON(Table table) {
        Gson gson = new Gson();
        return gson.toJson(table);
    }

    public NodeVersion createTable(Table table) throws InvalidObjectException, MetaException {
        if (table == null) {
            throw new InvalidObjectException("Database object passed is null");
        }
        try {
            String tableName = table.getTableName();
            Node dbNode = this.getNode(tableName);
            Structure dbStruct = this.getNodeStructure(tableName);

            Tag tableTag = new Tag("1.0.0", tableName, toJSON(table), GroundType.STRING);

            Map<String, GroundType> structVersionAttribs = new HashMap<>();
            structVersionAttribs.put(tableName, GroundType.STRING);

            StructureVersion sv = ground.getStructureVersionFactory().create(dbStruct.getId(), structVersionAttribs, EMPTY_PARENT_LIST);

            String reference = table.getDbName();
            HashMap<String, Tag> tags = new HashMap<>();
            tags.put(tableName, tableTag);

            Map<String, String> tableParamMap = table.getParameters();
            if (tableParamMap == null) {
                tableParamMap = new HashMap<String, String>();
            }
            Map<String, String> parameters = tableParamMap;

            List<String> parent = new ArrayList<String>();
            List<String> versions = ground.getNodeFactory().getLeaves(tableName);
            if (!versions.isEmpty()) {
                parent.add(versions.get(0));
            }

            NodeVersion dbNodeVersion = ground.getNodeVersionFactory().create(tags, sv.getId(), reference, parameters, dbNode.getId(), parent);

            return dbNodeVersion;
        } catch (GroundException e) {
            LOG.error("Failure to create a table node: " + table.getTableName());
            throw new MetaException(e.getMessage());
        }
    }

    public Table getTable(String dbName, String tableName) throws MetaException {
        try {
            List<String> versions = ground.getNodeFactory().getLeaves(tableName);
            if (versions.isEmpty()) {
                throw new MetaException("Table node not found: " + tableName);
            }

            NodeVersion latestVersion = ground.getNodeVersionFactory().retrieveFromDatabase(versions.get(0));
            Map<String, Tag> dbTag = latestVersion.getTags();

            return this.fromJSON((String) dbTag.get(tableName).getValue());
        } catch (GroundException ex) {
            throw new MetaException(ex.getMessage());
        }
    }

    public List<String> getTables(String dbName, String pattern) throws MetaException {
        List<String> tables = new ArrayList<String>();
        try {
            List<String> versions = ground.getNodeFactory().getLeaves(dbName);

            if (!versions.isEmpty()) {
                String metaVersionId = versions.get(0);
                List<String> tableNodeIds = ground.getNodeVersionFactory().getAdjacentNodes(metaVersionId, pattern);
                for (String tableNodeId : tableNodeIds) {
                    NodeVersion tableNodeVersion = ground.getNodeVersionFactory().retrieveFromDatabase(tableNodeId);
                    // create an edge for a dbname only if was not created
                    // earlier
                    Edge edge = ground.getEdgeFactory().retrieveFromDatabase(tableNodeVersion.getNodeId());
                    tables.add(edge.getName().split("Nodes.")[1]);
                }
            }
        } catch (GroundException ex) {
            LOG.error("Get tables failed for pattern {}", pattern);
            throw new MetaException(ex.getMessage());
        }
        return tables;
    }
}