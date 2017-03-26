/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.plugins.hive;

import edu.berkeley.ground.exceptions.GroundException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroundStore extends GroundStoreBase {

  private static final Logger LOG = LoggerFactory.getLogger(GroundStore.class.getName());

  // Do not access this directly, call getHBase to make sure it is
  // initialized.
  private GroundReadWrite ground = null;
  private GroundMetaStore metastore = null;
  private Configuration conf;
  private int txnNestLevel;

  public GroundStore() {
    ground = getGround();
    metastore = new GroundMetaStore(ground);
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
      if (txnNestLevel != 0) {
        rollbackTransaction();
      }
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
    if (db == null) {
      throw new InvalidObjectException("Invalid database object null");
    }

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
      LOG.error("Unable to create database: {}", e.getMessage());
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
      throw new MetaException("Unable to drop database " + dbName + " with error: "
          + e.getMessage());
    }
    LOG.info("database deleted: {}, {}", dbName);
    return true;
  }

  @Override
  public boolean alterDatabase(String dbname, Database db) throws NoSuchObjectException,
      MetaException {

    if (dbname == null || dbname.isEmpty() || db == null) {
      throw new NoSuchObjectException("Unable to locate database " + dbname + " with " + db);
    }

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

  /**
   * There would be a "database contains" relationship between D and T, and
   * there would be a "table contains" relationship between T and each
   * attribute. The types of attributes of those nodes, and the fact that A2
   * and A4 are partition keys would be tags of those nodes. The fact that the
   * table T is in a particular file format (Parquet or Avro) would be a tag
   * on the table node.
   */
  public void createTable(Table tbl) throws InvalidObjectException, MetaException {
    if (tbl == null) {
      throw new InvalidObjectException("Table passed is null");
    }

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
      LOG.error("Unable to add partition to table {} in database {}", part.getTableName(),
          part.getDbName());
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
  public Partition getPartition(String dbName, String tableName, List<String> partVals)
      throws MetaException, NoSuchObjectException {
    return this.metastore.getPartition(dbName, tableName, partVals.get(2));
  }

  @Override
  public boolean doesPartitionExist(String dbName, String tableName, List<String> partVals)
      throws MetaException, NoSuchObjectException {
    try {
      Partition partition = this.metastore.getPartition(dbName, tableName, partVals.get(2));
      return (partition != null);
    } catch (MetaException | NoSuchObjectException ex) {
      throw ex;
    }
  }

  @Override
  public boolean dropPartition(String dbName, String tableName, List<String> partVals)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    throw new MetaException("Drop partition is not currently supported");
  }

  @Override
  public List<Partition> getPartitions(String dbName, String tableName, int max)
      throws MetaException, NoSuchObjectException {
    try {
      return this.metastore.getPartitions(dbName, tableName, max);
    } catch (MetaException | NoSuchObjectException ex) {
      LOG.error("Get partitions failed table {} database {} error: {}", tableName, dbName,
          ex.getMessage());
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
      throw new MetaException("Invalid input to alter table " + tableName + " in database {}"
          + dbName);
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
  public List<String> getAllTables(String dbName) throws MetaException {
    return this.getTables(dbName, "");
  }

  @Override
  public List<String> listTableNamesByFilter(String dbName, String filter, short maxTables)
      throws MetaException, UnknownDBException {
    return metastore.getTables(dbName, filter);
  }

}
