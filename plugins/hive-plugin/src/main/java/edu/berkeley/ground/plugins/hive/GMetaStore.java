package edu.berkeley.ground.plugins.hive;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.berkeley.ground.api.models.Edge;
import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.NodeVersion;
import edu.berkeley.ground.api.models.Structure;
import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

public class GMetaStore {
    static final private Logger LOG = LoggerFactory.getLogger(GMetaStore.class.getName());

    private static final String DUMMY_NOT_USED = "DUMMY_NOT_USED";
    private static final String _TIMESTAMP = "_TIMESTAMP";
    static final String METASTORE_DATABASE_EDGE = "_METASTORE_DATABASE";
    static final String METASTORE_NODE = "_METASTORE";

    private GroundReadWrite ground = null;
    private GDatabase database = null;

    final String APACHE_HIVE_URL = "HIVE_INFO";
    HashMap<String, String> APACHE_HIVE_URL_MAP = new HashMap<String, String>();
    {
        APACHE_HIVE_URL_MAP.put("URL", "http://hive.apache.org/");
    }

    public GMetaStore(GroundReadWrite ground) {
        this.ground = ground;
        this.database = new GDatabase(ground);
    }

    public Node getNode() throws GroundException {
        try {
            LOG.debug("Fetching metastore node: " + METASTORE_NODE);
            return ground.getNodeFactory().retrieveFromDatabase(METASTORE_NODE);
        } catch (GroundException ge1) {
            LOG.debug("Not found - Creating metastore node: " + METASTORE_NODE);
            Node node = ground.getNodeFactory().create(METASTORE_NODE);
            Structure nodeStruct = ground.getStructureFactory().create(node.getName());

            return node;
        }
    }

    public Structure getNodeStructure() throws GroundException {
        try {
            LOG.debug("Fetching metastore node structure: " + METASTORE_NODE);
            Node node = this.getNode();
            return ground.getStructureFactory().retrieveFromDatabase(node.getName());
        } catch (GroundException e) {
            LOG.debug("Not found - metastore node structure: " + METASTORE_NODE);
            throw e;
        }
    }

    public Edge getEdge(NodeVersion nodeVersion) throws GroundException {
        String edgeId = nodeVersion.getNodeId();
        try {
            LOG.debug("Fetching metastore database edge: " + edgeId);
            return ground.getEdgeFactory().retrieveFromDatabase(edgeId);
        } catch (GroundException e) {
            LOG.debug("Not found - Creating metastore table edge: " + edgeId);
            Edge edge = ground.getEdgeFactory().create(edgeId);
            Structure edgeStruct = ground.getStructureFactory().create(edge.getName());
            return edge;
        }
    }

    public Structure getEdgeStructure(NodeVersion nodeVersion) throws GroundException {
        try {
            LOG.debug("Fetching metastore database edge structure: " + nodeVersion.getNodeId());
            Edge edge = this.getEdge(nodeVersion);
            return ground.getStructureFactory().retrieveFromDatabase(edge.getName());
        } catch (GroundException e) {
            LOG.debug("Not found - metastore table edge structure: " + nodeVersion.getNodeId());
            throw e;
        }
    }

    public NodeVersion getNodeVersion() throws GroundException {
        try {
            Node node = this.getNode();
            String nodeVersionId = ground.getNodeFactory().getLeaves(node.getName()).get(0);
            return ground.getNodeVersionFactory().retrieveFromDatabase(nodeVersionId);
        } catch (GroundException ex) {
            throw ex;
        }
    }

    public NodeVersion createNodeVersion() throws GroundException {
        try {
            Node node = this.getNode();
            String nodeId = node.getId();

            Map<String, GroundType> structureVersionAttributes = new HashMap<>();
            structureVersionAttributes.put(_TIMESTAMP, GroundType.STRING);

            Structure structure = this.getNodeStructure();
            StructureVersion sv = ground.getStructureVersionFactory().create(structure.getId(),
                    structureVersionAttributes, new ArrayList<>());

            Map<String, Tag> tags = new HashMap<>();
            String timestamp = Date.from(Instant.now()).toString();
            tags.put(METASTORE_NODE, new Tag(DUMMY_NOT_USED, _TIMESTAMP, timestamp, GroundType.STRING));

            List<String> parent = new ArrayList<String>();
            List<String> versions = ground.getNodeFactory().getLeaves(METASTORE_NODE);
            if (!versions.isEmpty()) {
                parent.add(versions.get(0));
            }

            // TODO: put version and other information in tags
            return ground.getNodeVersionFactory().create(tags, sv.getId(), nodeId, APACHE_HIVE_URL_MAP, nodeId, parent);
        } catch (GroundException ex) {
            LOG.error("Unable to initialize Ground Metastore: " + ex);
            throw ex;
        }
    }

    // Database related functions

    public Database getDatabase(String name) throws NoSuchObjectException {
        try {
            List<String> dbNames = this.getDatabases(name);
            if (dbNames.contains(name)) {
                return database.getDatabase(name);
            }
            return null;
        } catch (NoSuchObjectException e) {
            LOG.error("Unable to locate table {}", name);
            throw e;
        }
    }

    public List<String> getDatabases(String dbPattern) throws NoSuchObjectException {
        List<String> databases = new ArrayList<String>();
        try {
            List<String> versions = ground.getNodeFactory().getLeaves(METASTORE_NODE);

            if (!versions.isEmpty()) {
                String metaVersionId = versions.get(0);
                List<String> dbNodeIds = ground.getNodeVersionFactory().getAdjacentNodes(metaVersionId, dbPattern);
                for (String dbNodeId : dbNodeIds) {
                    NodeVersion dbNodeVersion = ground.getNodeVersionFactory().retrieveFromDatabase(dbNodeId);
                    // fetch the edge for the dbname
                    Edge edge = ground.getEdgeFactory().retrieveFromDatabase(dbNodeVersion.getNodeId());
                    databases.add(edge.getName().split("Nodes.")[1]);
                }
            }
        } catch (GroundException ex) {
            LOG.error("Get databases failed for pattern {}", dbPattern);
            throw new NoSuchObjectException(ex.getMessage());
        }
        return databases;
    }

    public void createDatabase(Database db) throws InvalidObjectException, MetaException {
        try {
            NodeVersion nv = database.createDatabase(db);

            List<String> versions = ground.getNodeFactory().getLeaves(METASTORE_NODE);

            NodeVersion metaNodeVersion = this.createNodeVersion();
            String metaVersionId = metaNodeVersion.getId();
            Edge edge = this.getEdge(nv);
            Structure structure = this.getEdgeStructure(nv);
            Map<String, GroundType> structVersionAttribs = new HashMap<>();
            for (String key : nv.getTags().keySet()) {
                structVersionAttribs.put(key, GroundType.STRING);
            }
            StructureVersion sv = ground.getStructureVersionFactory().create(structure.getId(), structVersionAttribs,
                    new ArrayList<>());

            ground.getEdgeVersionFactory().create(nv.getTags(), sv.getId(), nv.getReference(), nv.getParameters(),
                    edge.getId(), metaVersionId, nv.getId(), new ArrayList<String>());

            if (!versions.isEmpty()) {
                if (versions.size() != 0) {
                    String prevVersionId = versions.get(0);
                    List<String> nodeIds = ground.getNodeVersionFactory().getAdjacentNodes(prevVersionId, "");
                    for (String nodeId : nodeIds) {
                        NodeVersion oldNV = ground.getNodeVersionFactory().retrieveFromDatabase(nodeId);
                        edge = this.getEdge(oldNV);

                        structVersionAttribs = new HashMap<>();
                        for (String key : oldNV.getTags().keySet()) {
                            structVersionAttribs.put(key, GroundType.STRING);
                        }

                        // create an edge version for a dbname
                        sv = ground.getStructureVersionFactory().create(structure.getId(), structVersionAttribs,
                                new ArrayList<>());
                        ground.getEdgeVersionFactory().create(oldNV.getTags(), sv.getId(), oldNV.getReference(),
                                oldNV.getParameters(), edge.getId(), metaVersionId, oldNV.getId(),
                                new ArrayList<String>());
                    }
                }
            }

        } catch (InvalidObjectException | MetaException e) {
            throw e;
        } catch (GroundException e) {
            throw new MetaException("Failed to create database: " + db.getName() + " because " + e.getMessage());
        }
    }

    public boolean dropDatabase(String dbName) throws GroundException {
        try {
            boolean found = false;
            List<String> versions = ground.getNodeFactory().getLeaves(METASTORE_NODE);

            if (versions.isEmpty()) {
                LOG.error("Could not find datbase to drop named " + dbName);
                return false;
            } else {
                String prevVersionId = versions.get(0);
                List<String> nodeIds = ground.getNodeVersionFactory().getAdjacentNodes(prevVersionId, "");

                if (nodeIds.size() == 0) {
                    LOG.error("Failed to drop database {}", dbName);
                    return false;
                }
                NodeVersion metaNodeVersion = this.createNodeVersion();
                String metaVersionId = metaNodeVersion.getId();
                String dbNodeId = "Nodes." + dbName;

                for (String nodeId : nodeIds) {
                    NodeVersion oldNV = ground.getNodeVersionFactory().retrieveFromDatabase(nodeId);

                    if (!oldNV.getNodeId().equals(dbNodeId)) {
                        Edge edge = this.getEdge(oldNV);
                        Structure structure = this.getEdgeStructure(oldNV);

                        LOG.error("Found edge with name {}", oldNV.getNodeId());

                        Map<String, GroundType> structVersionAttribs = new HashMap<>();
                        for (String key : oldNV.getTags().keySet()) {
                            structVersionAttribs.put(key, GroundType.STRING);
                        }
                        // create an edge for each dbname other than the one
                        // being deleted
                        StructureVersion sv = ground.getStructureVersionFactory().create(structure.getId(),
                                structVersionAttribs, new ArrayList<>());
                        ground.getEdgeVersionFactory().create(oldNV.getTags(), sv.getId(), oldNV.getReference(),
                                oldNV.getParameters(), edge.getId(), metaVersionId, oldNV.getId(),
                                new ArrayList<String>());
                    } else {
                        found = true;
                    }
                }
            }
            return found;
        } catch (GroundException e) {
            LOG.error("Failed to drop database {}", dbName);
            throw e;
        }
    }

    // Table related functions
    public void createTable(Table table) throws InvalidObjectException, MetaException {
        try {
            String dbName = table.getDbName();

            NodeVersion nv = database.createTable(table);

            List<String> versions = ground.getNodeFactory().getLeaves(METASTORE_NODE);

            NodeVersion metaNodeVersion = this.createNodeVersion();
            String metaVersionId = metaNodeVersion.getId();
            Edge edge = this.getEdge(nv);
            Structure structure = this.getEdgeStructure(nv);
            Map<String, GroundType> structVersionAttribs = new HashMap<>();
            for (String key : nv.getTags().keySet()) {
                structVersionAttribs.put(key, GroundType.STRING);
            }
            StructureVersion sv = ground.getStructureVersionFactory().create(structure.getId(), structVersionAttribs,
                    new ArrayList<>());

            ground.getEdgeVersionFactory().create(nv.getTags(), sv.getId(), nv.getReference(), nv.getParameters(),
                    edge.getId(), metaVersionId, nv.getId(), new ArrayList<String>());

            String dbNodeId = "Nodes." + dbName;

            if (!versions.isEmpty()) {
                if (versions.size() != 0) {
                    String prevVersionId = versions.get(0);
                    List<String> nodeIds = ground.getNodeVersionFactory().getAdjacentNodes(prevVersionId, "");
                    for (String nodeId : nodeIds) {
                        NodeVersion oldNV = ground.getNodeVersionFactory().retrieveFromDatabase(nodeId);
                        edge = this.getEdge(oldNV);

                        if (!oldNV.getNodeId().equals(dbNodeId)) {
                            structVersionAttribs = new HashMap<>();
                            for (String key : oldNV.getTags().keySet()) {
                                structVersionAttribs.put(key, GroundType.STRING);
                            }

                            // create an edge version for a dbname
                            sv = ground.getStructureVersionFactory().create(structure.getId(), structVersionAttribs,
                                    new ArrayList<>());
                            ground.getEdgeVersionFactory().create(oldNV.getTags(), sv.getId(), oldNV.getReference(),
                                    oldNV.getParameters(), edge.getId(), metaVersionId, oldNV.getId(),
                                    new ArrayList<String>());
                        }
                    }
                }
            }

        } catch (InvalidObjectException | MetaException e) {
            throw e;
        } catch (GroundException e) {
            throw new MetaException("Failed to create table: " + table.getTableName() + " because " + e.getMessage());
        }
    }

    public boolean dropTable(String dbName, String tableName)
            throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        try {
            NodeVersion nv = database.dropTable(dbName, tableName);
            if (nv == null) {
                throw new NoSuchObjectException("Table not found: " + tableName);
            }

            List<String> versions = ground.getNodeFactory().getLeaves(METASTORE_NODE);

            NodeVersion metaNodeVersion = this.createNodeVersion();
            String metaVersionId = metaNodeVersion.getId();
            Edge edge = this.getEdge(nv);
            Structure structure = this.getEdgeStructure(nv);
            Map<String, GroundType> structVersionAttribs = new HashMap<>();
            for (String key : nv.getTags().keySet()) {
                structVersionAttribs.put(key, GroundType.STRING);
            }
            StructureVersion sv = ground.getStructureVersionFactory().create(structure.getId(), structVersionAttribs,
                    new ArrayList<>());

            ground.getEdgeVersionFactory().create(nv.getTags(), sv.getId(), nv.getReference(), nv.getParameters(),
                    edge.getId(), metaVersionId, nv.getId(), new ArrayList<String>());

            String dbNodeId = "Nodes." + dbName;

            if (!versions.isEmpty()) {
                if (versions.size() != 0) {
                    String prevVersionId = versions.get(0);
                    List<String> nodeIds = ground.getNodeVersionFactory().getAdjacentNodes(prevVersionId, "");
                    for (String nodeId : nodeIds) {
                        NodeVersion oldNV = ground.getNodeVersionFactory().retrieveFromDatabase(nodeId);
                        edge = this.getEdge(oldNV);

                        if (!oldNV.getNodeId().equals(dbNodeId)) {
                            structVersionAttribs = new HashMap<>();
                            for (String key : oldNV.getTags().keySet()) {
                                structVersionAttribs.put(key, GroundType.STRING);
                            }

                            // create an edge version for a dbname
                            sv = ground.getStructureVersionFactory().create(structure.getId(), structVersionAttribs,
                                    new ArrayList<>());
                            ground.getEdgeVersionFactory().create(oldNV.getTags(), sv.getId(), oldNV.getReference(),
                                    oldNV.getParameters(), edge.getId(), metaVersionId, oldNV.getId(),
                                    new ArrayList<String>());
                        }
                    }
                }
            }
            return true;
        } catch (InvalidObjectException | MetaException e) {
            throw e;
        } catch (GroundException e) {
            throw new MetaException("Failed to create table: " + tableName + " because " + e.getMessage());
        }
    }

    public Table getTable(String dbName, String tableName) throws MetaException {
        try {
            return database.getTable(dbName, tableName);
        } catch (MetaException ex) {
            LOG.error("Unalbe to find table {} for database {}", tableName, dbName);
            throw ex;
        }
    }

    public List<String> getTables(String dbName, String pattern) throws MetaException {
        return database.getTables(dbName, pattern);
    }

    public boolean addPartitions(String dbName, String tableName, List<Partition> parts)
            throws InvalidObjectException, MetaException {
        try {
            NodeVersion nv = database.addPartitions(dbName, tableName, parts);
            if (nv == null) {
                LOG.error("Unable to create partition for table " + tableName + " in database " + dbName);
                throw new InvalidObjectException(
                        "Unable to create partition for table " + tableName + " in database " + dbName);
            }

            List<String> versions = ground.getNodeFactory().getLeaves(METASTORE_NODE);

            NodeVersion metaNodeVersion = this.createNodeVersion();
            String metaVersionId = metaNodeVersion.getId();
            Edge edge = this.getEdge(nv);
            Structure structure = this.getEdgeStructure(nv);
            Map<String, GroundType> structVersionAttribs = new HashMap<>();
            for (String key : nv.getTags().keySet()) {
                structVersionAttribs.put(key, GroundType.STRING);
            }
            StructureVersion sv = ground.getStructureVersionFactory().create(structure.getId(), structVersionAttribs,
                    new ArrayList<>());

            ground.getEdgeVersionFactory().create(nv.getTags(), sv.getId(), nv.getReference(), nv.getParameters(),
                    edge.getId(), metaVersionId, nv.getId(), new ArrayList<String>());

            String dbNodeId = "Nodes." + dbName;

            if (!versions.isEmpty()) {
                if (versions.size() != 0) {
                    String prevVersionId = versions.get(0);
                    List<String> nodeIds = ground.getNodeVersionFactory().getAdjacentNodes(prevVersionId, "");
                    for (String nodeId : nodeIds) {
                        NodeVersion oldNV = ground.getNodeVersionFactory().retrieveFromDatabase(nodeId);
                        edge = this.getEdge(oldNV);

                        if (!oldNV.getNodeId().equals(dbNodeId)) {
                            structVersionAttribs = new HashMap<>();
                            for (String key : oldNV.getTags().keySet()) {
                                structVersionAttribs.put(key, GroundType.STRING);
                            }

                            // create an edge version for a dbname
                            sv = ground.getStructureVersionFactory().create(structure.getId(), structVersionAttribs,
                                    new ArrayList<>());
                            ground.getEdgeVersionFactory().create(oldNV.getTags(), sv.getId(), oldNV.getReference(),
                                    oldNV.getParameters(), edge.getId(), metaVersionId, oldNV.getId(),
                                    new ArrayList<String>());
                        }
                    }
                }
            }
            return true;

        } catch (InvalidObjectException | MetaException ex) {
            LOG.error("Unable to add partition to table {} database {} with error: ", tableName, dbName,
                    ex.getMessage());
            throw ex;
        } catch (GroundException e) {
            throw new MetaException(
                    "Failed to add partition to " + tableName + " in " + dbName + " because " + e.getMessage());
        }
    }

    public Partition getPartition(String dbName, String tableName, String partName)
            throws MetaException, NoSuchObjectException {
        return this.database.getPartition(dbName, tableName, partName);
    }

    public List<Partition> getPartitions(String dbName, String tableName, int max)
            throws MetaException, NoSuchObjectException {
        try {
            return this.database.getPartitions(dbName, tableName, max);
        } catch (MetaException | NoSuchObjectException ex) {
            throw ex;
        }
    }
}
