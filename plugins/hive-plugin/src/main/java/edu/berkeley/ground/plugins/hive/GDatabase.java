package edu.berkeley.ground.plugins.hive;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hive.common.util.HiveStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import edu.berkeley.ground.api.models.Edge;
import edu.berkeley.ground.api.models.EdgeVersion;
import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.NodeFactory;
import edu.berkeley.ground.api.models.NodeVersion;
import edu.berkeley.ground.api.models.NodeVersionFactory;
import edu.berkeley.ground.api.models.Structure;
import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.plugins.hive.GroundStore.EntityState;

public class GDatabase {
    static final private Logger LOG = LoggerFactory.getLogger(GDatabase.class.getName());

    static final String DATABASE_NODE = "_DATABASE";

    static final String DATABASE_TABLE_EDGE = "_DATABASE_TABLE";

    private static final List<String> EMPTY_PARENT_LIST = new ArrayList<String>();
    
    private GroundReadWrite ground = null;

    public GDatabase(GroundReadWrite ground) {
        this.ground = ground;
    }

    public Node getNode(String dbName) throws GroundException {
        try {
            LOG.debug("Fetching database node: " + dbName);
            return ground.getNodeFactory().retrieveFromDatabase(dbName);
        } catch (GroundException ge1) {
            LOG.debug("Not found - Creating databsae node: " + dbName);

            Node node = ground.getNodeFactory().create(dbName);
            Structure nodeStruct = ground.getStructureFactory().create(node.getName());

            return node;
        }
    }

    public Structure getNodeStructure(String dbName) throws GroundException {
        try {
            Node node = this.getNode(dbName);     
            return ground.getStructureFactory().retrieveFromDatabase(dbName);
        } catch (GroundException e) {
            LOG.error("Unable to fetch database node structure");
            throw e;
        }
    }

    public Edge getEdge(String tableName) throws GroundException {
        try {
            LOG.debug("Fetching database table edge: " + tableName);
            return ground.getEdgeFactory().retrieveFromDatabase(tableName);
        } catch (GroundException ge1) {
            LOG.debug("Not found - Creating databsae table edge: " + tableName);

            Edge edge = ground.getEdgeFactory().create(tableName);
            Structure edgeStruct = ground.getStructureFactory().create(tableName);
            return edge;
        }
    }

    public Structure getEdgeStructure(String tableName) throws GroundException {
        try {
            Edge edge = getEdge(tableName);     
            return ground.getStructureFactory().retrieveFromDatabase(tableName);
        } catch (GroundException e) {
            LOG.error("Unable to fetch database tabe edge structure");
            throw e;
        }
    }

    public Database fromJSON(String json) {
        Gson gson = new Gson();
        return (Database) gson.fromJson(json, Database.class);
    }

    public String toJSON(Database db) {
        Gson gson = new Gson();
        return gson.toJson(db);
    }

    public Database getDatabase(String dbName) throws NoSuchObjectException {
        try {
            List<String> versions = ground.getNodeFactory().getLeaves(dbName);
            if (versions.isEmpty()) {
                throw new GroundException("Database node not found: " + dbName);
            }
            
            NodeVersion latestVersion = ground.getNodeVersionFactory().retrieveFromDatabase(versions.get(0));
            Map<String, Tag> dbTag = latestVersion.getTags();

            return this.fromJSON((String) dbTag.get(dbName).getValue());
        } catch (GroundException e) {
            throw new NoSuchObjectException(e.getMessage());
        }
    }
    
    public NodeVersion createDatabase(Database db) throws InvalidObjectException, MetaException {
        if (db == null) {
            throw new InvalidObjectException("Database object passed is null");
        }
        try {
            String dbName = db.getName();
            Node dbNode = this.getNode(dbName);
            Structure dbStruct = this.getNodeStructure(dbName);
            
            Tag dbTag = new Tag("1.0.0", dbName, toJSON(db), GroundType.STRING);
            
            Map<String, GroundType> structVersionAttribs = new HashMap<>();
            structVersionAttribs.put(dbName, GroundType.STRING);
            
            StructureVersion sv = ground.getStructureVersionFactory().create(dbStruct.getId(), structVersionAttribs, EMPTY_PARENT_LIST);

            String reference = db.getLocationUri();
            HashMap<String, Tag> tags = new HashMap<>();
            tags.put(dbName, dbTag);

            Map<String, String> dbParamMap = db.getParameters();
            if (dbParamMap == null) {
                dbParamMap = new HashMap<String, String>();
            }
            Map<String, String> parameters = dbParamMap;

            NodeVersion dbNodeVersion = ground.getNodeVersionFactory().create(tags, sv.getId(), reference, parameters, dbNode.getId(), EMPTY_PARENT_LIST);
            
            return dbNodeVersion;
        } catch (GroundException e) {
            LOG.error("Failure to create a database node: " + db.getName());
            throw new MetaException(e.getMessage());
        }
    }
}
