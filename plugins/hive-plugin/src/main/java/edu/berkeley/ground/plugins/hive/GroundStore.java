/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.ground.plugins.hive;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.berkeley.ground.exceptions.GroundException;

/** RawStore Implementation using Ground APIs. */
public class GroundStore extends GroundStoreBase {

    static final private Logger LOG = LoggerFactory.getLogger(GroundStore.class.getName());

    // Do not access this directly, call getHBase to make sure it is
    // initialized.
    private GroundReadWrite ground;
    private GroundDatabase groundDatabase;
    private GroundTable groundTable;
    private Configuration conf;
    private int txnNestLevel;

    public static enum EntityState {
        ACTIVE, DELETED
    }

    public GroundStore() throws GroundException {
        ground = getGround();
        groundDatabase = new GroundDatabase(ground);
        groundTable = new GroundTable(ground);
        new GroundPartition(ground);
    }

    private GroundReadWrite getGround() throws GroundException {
        if (ground == null) {
            this.ground = new GroundReadWrite();
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
        if (txnNestLevel != 0)
            rollbackTransaction();
    }

    @Override
    public boolean openTransaction() {
        if (txnNestLevel++ <= 0) {
            LOG.debug("Opening Ground transaction");
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
     *
     * @throws MetaException
     * @throws InvalidObjectException
     */
    @Override
    public void createDatabase(Database db) throws MetaException, InvalidObjectException {
        if (db == null)
            throw new InvalidObjectException("Invalid database object null");

        try {
            Database database = groundDatabase.getDatabase(db.getName());
            if (database != null) {
                LOG.error("Database already exists: {}", db.getName());
                return;
            }
        } catch (GroundException e) {
            // ignore if the database does not exist
        }
        try {
            groundDatabase.createDatabaseNodeVersion(db, EntityState.ACTIVE.name());
        } catch (InvalidObjectException | MetaException e) {
            LOG.error("Unable to create database: {}", e.getMessage());
            throw new MetaException(e.getMessage());
        }
    }

    @Override
    public Database getDatabase(String dbName) throws NoSuchObjectException {
        Database database = null;
        try {
            database = groundDatabase.getDatabase(dbName);
            return database;
        } catch (GroundException e) {
            throw new NoSuchObjectException("Database not found: " + dbName);
        }
    }

    @Override
    public boolean dropDatabase(String dbName) throws NoSuchObjectException, MetaException {
        try {
            groundDatabase.dropDatabase(dbName, EntityState.DELETED.name());
        } catch (GroundException e) {
            throw new MetaException(e.getMessage());
        }
        LOG.info("database deleted: {}", dbName);
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
            LOG.debug("Alter database failed with: {}", ex.getMessage());
            throw ex;
        } catch (InvalidObjectException ex) {
            LOG.debug("Alter database failed with: " + ex.getMessage());
            throw new MetaException("Alter database failed: " + ex.getMessage());
        }
        return false;
    }

    @Override
    public List<String> getDatabases(String pattern) throws MetaException {
        return groundDatabase.getDatabases(pattern);
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

    /**
     * There would be a "database contains" relationship between D and T, and
     * there would be a "table contains" relationship between T and each
     * attribute. The types of attributes of those nodes, and the fact that A2
     * and A4 are partition keys would be tags of those nodes. The fact that the
     * table T is in a particular file format (Parquet or Avro) would be a tag
     * on the table node.
     */
    @Override
    public void createTable(Table tbl) throws InvalidObjectException, MetaException {
        if (tbl == null)
            throw new InvalidObjectException("Table passed is null");
        try {
            this.getDatabase(tbl.getDbName());
            this.groundTable.createTableNodeVersion(tbl);
        } catch (NoSuchObjectException ex) {
            throw new MetaException(ex.getMessage());
        }
    }

    @Override
    public boolean dropTable(String dbName, String tableName)
            throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        try {
            return this.groundTable.dropTable(dbName, tableName, EntityState.DELETED.name());
        } catch (GroundException e) {
            throw new MetaException(e.getMessage());
        }
    }

    @Override
    public Table getTable(String dbName, String tableName) throws MetaException {
        return this.groundTable.getTable(dbName, tableName);
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
            return this.groundTable.addPartitions(dbName, tableName, parts);
        } catch (InvalidObjectException | MetaException ex) {
            LOG.error("Unable to add partition to table {} database {} with error: {}", tableName, dbName,
                    ex.getMessage());
            throw ex;
        }
    }

    @Override
    public List<Partition> getPartitionsByNames(String dbName, String tableName, List<String> part_vals)
            throws MetaException, NoSuchObjectException {
        return this.groundTable.getPartitions(dbName, tableName, part_vals);
    }

    @Override
    public boolean doesPartitionExist(String dbName, String tableName, List<String> part_vals)
            throws MetaException, NoSuchObjectException {
        try {
            List<Partition> partition = this.groundTable.getPartitions(dbName, tableName, part_vals);
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
            return this.groundTable.getPartitions(dbName, tableName, max);
        } catch (MetaException | NoSuchObjectException ex) {
            LOG.error("Get partitions failed table {} database {} error: {}", tableName, dbName, ex.getMessage());
            throw ex;
        }
    }

    @Override
    public void alterTable(String dbName, String tableName, Table newTable)
            throws InvalidObjectException, MetaException {
        try {
            this.groundTable.dropTable(dbName, tableName, EntityState.DELETED.name());
            this.groundTable.createTableNodeVersion(newTable);
        } catch (GroundException e) {
            // TODO Auto-generated catch block
            throw new MetaException(e.getMessage());
        }
    }

    @Override
    public List<String> getTables(String dbName, String pattern) throws MetaException {
        return groundTable.getTables(dbName, pattern);
    }

    @Override
    public List<String> getAllTables(String dbName) throws MetaException {
        return this.getTables(dbName, "");
    }

    @Override
    public List<String> listTableNamesByFilter(String dbName, String filter, short max_tables)
            throws MetaException, UnknownDBException {
        return groundTable.getTables(dbName, filter);
    }

}
