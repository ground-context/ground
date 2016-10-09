package edu.berkeley.ground.plugins.hive;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.*; //TODO(krishna) fix
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.FileMetadataHandler;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.PartFilterExprUtil;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import edu.berkeley.ground.api.models.Edge;
import edu.berkeley.ground.api.models.EdgeFactory;
import edu.berkeley.ground.api.models.EdgeVersion;
import edu.berkeley.ground.api.models.EdgeVersionFactory;
import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.NodeFactory;
import edu.berkeley.ground.api.models.NodeVersion;
import edu.berkeley.ground.api.models.NodeVersionFactory;
import edu.berkeley.ground.api.models.Structure;
import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

public class GroundStore extends GroundStoreBase {

    private static final String DEFAULT_VERSION = "1.0.0";

    static final private Logger LOG = LoggerFactory.getLogger(GroundStore.class.getName());

    private static final String METASTORE_NODE = "_METASTORE";

    private static final String DB_STATE = "_DATABASE_STATE";

    private static final String TABLE_STATE = "_TABLE_STATE";

    private static final String TIMESTAMP = "_TIMESTAMP";

    // Do not access this directly, call getHBase to make sure it is
    // initialized.
    private GroundReadWrite ground = null;
    private GMetaStore metastore = null;
    private Configuration conf;
    private int txnNestLevel;

    public static enum EntityState {
        ACTIVE, DELETED;
    }

    public GroundStore() {
        ground = getGround();
        metastore = new GMetaStore(ground);
    }

    private GroundReadWrite getGround() {
        if (ground == null) {
            if (conf == null) {
                conf = new HiveConf();
            }
            GroundReadWrite.setConf(conf);
            this.ground = GroundReadWrite.getInstance();
        }
        return ground;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration configuration) {
        conf = configuration;
    }

    @Override
    public void shutdown() {
        try {
            if (txnNestLevel != 0)
                rollbackTransaction();
            getGround().close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean openTransaction() {
        if (txnNestLevel++ <= 0) {
            LOG.debug("Opening Ground transaction");
            getGround().begin();
            txnNestLevel = 1;
        }
        return true;
    }

    @Override
    public boolean commitTransaction() {
        return true;
        // throw new UnsupportedOperationException();
    }

    @Override
    public void rollbackTransaction() {
        // throw new UnsupportedOperationException();
    }

    /**
     * create a database using ground APIs. Uses node and node version.
     */
    @Override
    public void createDatabase(Database db) throws InvalidObjectException, MetaException {
        if (db == null)
            throw new InvalidObjectException("Invalid database object null");

        try {
            Database database = metastore.getDatabase(db.getName());
            if (database != null) {
                throw new MetaException("Database already exists: " + db.getName());
            }
        } catch (NoSuchObjectException e) {
            // ignore if the database does not exist
        }
        try {
            metastore.createDatabase(db);
        } catch (InvalidObjectException | MetaException e) {
            LOG.error("Unable to create database: " + e.getMessage());
            throw e;
        }
    }

    @Override
    public Database getDatabase(String dbName) throws NoSuchObjectException {
        Database database = metastore.getDatabase(dbName);
        if (database == null) {
            LOG.info("database node version is not present");
            throw new NoSuchObjectException("Database not found: " + dbName);
        }
        return database;
    }

    @Override
    public boolean dropDatabase(String dbName) throws NoSuchObjectException, MetaException {
        try {
            metastore.dropDatabase(dbName);
        } catch (GroundException e) {
            throw new MetaException("Unable to drop database " + dbName + " with error: " + e.getMessage());
        }
        LOG.info("database deleted: {}, {}", dbName);
        return true;
    }

    @Override
    public boolean alterDatabase(String dbname, Database db) throws NoSuchObjectException, MetaException {
        if (dbname == null || dbname.isEmpty() || db == null)
            throw new NoSuchObjectException("Unable to locate database " + dbname + " with " + db);
        try {
            dropDatabase(dbname);
            createDatabase(db);
        } catch (NoSuchObjectException | MetaException ex) {
            LOG.debug("Alter database failed with: " + ex.getMessage());
            throw ex;
        } catch (InvalidObjectException ex) {
            LOG.debug("Alter database failed with: " + ex.getMessage());
            throw new MetaException("Alter database failed: " + ex.getMessage());
        }
        return false;
    }

    @Override
    public List<String> getDatabases(String pattern) throws MetaException {
        try {
            return metastore.getDatabases(pattern);
        } catch (NoSuchObjectException ex) {
            LOG.error("Failed to get databases with pattern {}", pattern);
            throw new MetaException(ex.getMessage());
        }
    }

    @Override
    public List<String> getAllDatabases() throws MetaException {
        try {
            return this.getDatabases("");
        } catch (MetaException ex) {
            LOG.error("Failed to get all databases");
            throw new MetaException(ex);
        }
    }

    @Override
    public boolean createType(Type type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Type getType(String typeName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean dropType(String typeName) {
        throw new UnsupportedOperationException();
    }

    /** Given an entity name retrieve its node version from database. */
    private NodeVersion getNodeVersion(String name) throws NoSuchObjectException {
        try {
            LOG.info("getting node versions for {}", name);
            List<String> versions = getGround().getNodeFactory().getLeaves(name);
            if (versions == null || versions.isEmpty())
                return null;
            return getGround().getNodeVersionFactory().retrieveFromDatabase(versions.get(0));
        } catch (GroundException e) {
            LOG.error("get failed for database {}", name);
            throw new NoSuchObjectException(e.getMessage());
        }
    }

    private StructureVersion createStructureVersion(String name, List<String> parentIds) throws GroundException {
        Map<String, GroundType> structureVersionAttributes = new HashMap<>();
        structureVersionAttributes.put(name, GroundType.STRING);
        for (String parentId : parentIds) {
            structureVersionAttributes.put(parentId, GroundType.STRING);
        }
        Structure structure = getGround().getStructureFactory().create(name);
        StructureVersion sv = getGround().getStructureVersionFactory().create(structure.getId(),
                structureVersionAttributes, parentIds);
        return sv;
    }

    /**
     *
     * There would be a "database contains" relationship between D and T, and
     * there would be a "table contains" relationship between T and each
     * attribute. The types of attributes of those nodes, and the fact that A2
     * and A4 are partition keys would be tags of those nodes. The fact that the
     * table T is in a particular file format (Parquet or Avro) would be a tag
     * on the table node.
     * 
     */
    public void createTable(Table tbl) throws InvalidObjectException, MetaException {
        if (tbl == null)
            throw new InvalidObjectException("Table passed is null");
        try {
            this.getDatabase(tbl.getDbName());
            this.metastore.createTable(tbl);
        } catch (NoSuchObjectException ex) {
            throw new MetaException(ex.getMessage());
        }
    }

    @Override
    public boolean dropTable(String dbName, String tableName)
            throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        return this.metastore.dropTable(dbName, tableName);
    }

    @Override
    public Table getTable(String dbName, String tableName) throws MetaException {
        return this.metastore.getTable(dbName, tableName);
    }

    @Override
    public boolean addPartition(Partition part) throws InvalidObjectException, MetaException {
        try {
            List<Partition> parts = new ArrayList<Partition>();
            parts.add(part);
            return this.addPartitions(part.getDbName(), part.getTableName(), parts);
        } catch (MetaException ex) {
            LOG.error("Unable to add partition to table {} in database {}", part.getTableName(), part.getDbName());
            throw new MetaException("Unable to add partition: " + ex.getMessage());
        } catch (InvalidObjectException ex) {
            LOG.error("Invalid input - add partition failed");
            throw new InvalidObjectException("Invalid input - add partition failed: " + ex.getMessage());
        }
    }

    @Override
    public boolean addPartitions(String dbName, String tableName, List<Partition> parts)
            throws InvalidObjectException, MetaException {
        try {
            return this.metastore.addPartitions(dbName, tableName, parts);
        } catch (InvalidObjectException | MetaException ex) {
            LOG.error("Unable to add partition to table {} database {} with error: {}", tableName, dbName,
                    ex.getMessage());
            throw ex;
        }
    }

    @Override
    public Partition getPartition(String dbName, String tableName, List<String> part_vals)
            throws MetaException, NoSuchObjectException {
        return this.metastore.getPartition(dbName, tableName, part_vals.get(2));
    }

    @Override
    public boolean doesPartitionExist(String dbName, String tableName, List<String> part_vals)
            throws MetaException, NoSuchObjectException {
        try {
            Partition partition = this.metastore.getPartition(dbName, tableName, part_vals.get(2));
            return (partition != null);
        } catch (MetaException | NoSuchObjectException ex) {
            throw ex;
        }
    }

    @Override
    public boolean dropPartition(String dbName, String tableName, List<String> part_vals)
            throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        throw new MetaException("Drop partition is not currently supported");
    }

    @Override
    public List<Partition> getPartitions(String dbName, String tableName, int max)
            throws MetaException, NoSuchObjectException {
        try {
            return this.metastore.getPartitions(dbName, tableName, max);
        } catch (MetaException | NoSuchObjectException ex) {
            LOG.error("Get partitions failed table {} database {} error: {}", tableName, dbName, ex.getMessage());
            throw ex;
        }
    }

    @Override
    public void alterTable(String dbName, String tableName, Table newTable)
            throws InvalidObjectException, MetaException {
        try {
            this.metastore.dropTable(dbName, tableName);
        } catch (MetaException | InvalidObjectException ex) {
            LOG.error("Unable to drop previous version of table {} in database {}", tableName, dbName);
            throw ex;
        } catch (NoSuchObjectException ex) {
            LOG.error("Unable to drop previous version of table {} in database {}", tableName, dbName);
            throw new MetaException("Table " + tableName + " not found in database" + dbName);
        } catch (InvalidInputException ex) {
            LOG.error("Invalid input to alter table {} in database {}", tableName, dbName);
            throw new MetaException("Invalid input to alter table " + tableName + " in database {}" + dbName);
        }
        try {
            this.metastore.createTable(newTable);
        } catch (InvalidObjectException | MetaException ex) {
            LOG.error("Unable to alter table {} in database {}", tableName, dbName);
            throw ex;
        }
    }

    @Override
    public List<String> getTables(String dbName, String pattern) throws MetaException {
        return metastore.getTables(dbName, pattern);
    }

    @Override
    public List<TableMeta> getTableMeta(String dbNames, String tableNames, List<String> tableTypes)
            throws MetaException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<Table> getTableObjectsByName(String dbname, List<String> tableNames)
            throws MetaException, UnknownDBException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<String> getAllTables(String dbName) throws MetaException {
        return this.getTables(dbName, "");
    }

    @Override
    public List<String> listTableNamesByFilter(String dbName, String filter, short max_tables)
            throws MetaException, UnknownDBException {
        return metastore.getTables(dbName, filter);
    }

    @Override
    public List<String> listPartitionNames(String db_name, String tbl_name, short max_parts) throws MetaException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<String> listPartitionNamesByFilter(String db_name, String tbl_name, String filter, short max_parts)
            throws MetaException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void alterPartition(String db_name, String tbl_name, List<String> part_vals, Partition new_part)
            throws InvalidObjectException, MetaException {
        // TODO Auto-generated method stub
    }

    @Override
    public void alterPartitions(String db_name, String tbl_name, List<List<String>> part_vals_list,
            List<Partition> new_parts) throws InvalidObjectException, MetaException {
        // TODO Auto-generated method stub

    }

    @Override
    public List<Partition> getPartitionsByFilter(String dbName, String tblName, String filter, short maxParts)
            throws MetaException, NoSuchObjectException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean getPartitionsByExpr(String dbName, String tblName, byte[] expr, String defaultPartitionName,
            short maxParts, List<Partition> result) throws TException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getNumPartitionsByFilter(String dbName, String tblName, String filter)
            throws MetaException, NoSuchObjectException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public List<Partition> getPartitionsByNames(String dbName, String tblName, List<String> partNames)
            throws MetaException, NoSuchObjectException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Table markPartitionForEvent(String dbName, String tblName, Map<String, String> partVals,
            PartitionEventType evtType)
            throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isPartitionMarkedForEvent(String dbName, String tblName, Map<String, String> partName,
            PartitionEventType evtType)
            throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void dropPartitions(String dbName, String tblName, List<String> partNames)
            throws MetaException, NoSuchObjectException {
        // TODO Auto-generated method stub

    }

}
