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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Deadline;
import org.apache.hadoop.hive.metastore.FileFormatProxy;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.junit.After;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;


import edu.berkeley.ground.api.models.postgres.PostgresEdgeVersionFactory;
import edu.berkeley.ground.api.models.postgres.PostgresGraphVersionFactory;
import edu.berkeley.ground.api.models.postgres.PostgresNodeVersionFactory;

public class GroundMetastoreTest {
  private GroundStore groundStore = null;
  static final String GRAPHFACTORY_CLASS = "ground.graph.factory";

  static final String NODEFACTORY_CLASS = "ground.node.factory";

  static final String EDGEFACTORY_CLASS = "ground.edge.factory";

  private static final String DB1 = "testgroundstoredb1";
  private static final String DB2 = "testgroundstoredb2";
  private static final String TABLE1 = "testgroundstoretable1";
  private static final String KEY1 = "testgroundstorekey1";
  private static final String KEY2 = "testgroundstorekey2";
  private static final String OWNER = "testgroundstoreowner";
  private static final String USER1 = "testgroundstoreuser1";
  private static final String ROLE1 = "testgroundstorerole1";
  private static final String ROLE2 = "testgroundstorerole2";
  private static final String DBTBL1 = "testgroundstoredb1withtable";
  private static final String DBPART1 = "testgroundstoredbpart1";
  private static final String PARTTABLE1 = "partitiontesttable1";

  public static class MockPartitionExpressionProxy implements PartitionExpressionProxy {
    @Override
    public String convertExprToFilter(byte[] expr) throws MetaException {
      return null;
    }

    @Override
    public boolean filterPartitionsByExpr(List<String> partColumnNames, List<PrimitiveTypeInfo> partColumnTypeInfos,
                                          byte[] expr, String defaultPartitionName, List<String> partitionNames) throws MetaException {
      return false;
    }

    @Override
    public FileMetadataExprType getMetadataType(String inputFormat) {
      return null;
    }

    @Override
    public SearchArgument createSarg(byte[] expr) {
      return null;
    }

    @Override
    public FileFormatProxy getFileFormatProxy(FileMetadataExprType type) {
      return null;
    }
  }

  @Before
  public void setUp() throws Exception {
    HiveConf conf = new HiveConf();
    Deadline.registerIfNot(100000);
    conf.setVar(HiveConf.ConfVars.METASTORE_EXPRESSION_PROXY_CLASS, MockPartitionExpressionProxy.class.getName());
    HiveConf.setVar(conf, HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER, "test_connection");
    conf.set(GRAPHFACTORY_CLASS, PostgresGraphVersionFactory.class.getName());
    conf.set(NODEFACTORY_CLASS, PostgresNodeVersionFactory.class.getName());
    conf.set(EDGEFACTORY_CLASS, PostgresEdgeVersionFactory.class.getName());
    groundStore = new GroundStore();
    groundStore.setConf(conf);
    // dropAllStoreObjects(groundStore);
  }

  @After
  public void tearDown() {
  }

  /**
   * Test database operations
   */
  @Test
  public void testDatabaseOps() throws MetaException, InvalidObjectException, NoSuchObjectException {
    //int numDBs = groundStore.getAllDatabases().size();
    int numDBs = 0;
    Database db1 = new Database(DB1, "description", "locationurl", new HashMap<String, String>());
    Database db2 = new Database(DB2, "description", "locationurl", new HashMap<String, String>());
    groundStore.createDatabase(db1);
    numDBs++;
    groundStore.createDatabase(db2);
    numDBs++;

    String dbName = groundStore.getDatabase(DB1).getName();
    assertEquals(DB1, dbName);
    dbName = groundStore.getDatabase(DB2).getName();
    assertEquals(DB2, dbName);
    List<String> databases = groundStore.getAllDatabases();
    assertEquals(numDBs, databases.size());
    for (String database : databases) {
      System.out.println("database: " + database);
    }
    assertTrue(databases.contains(DB2));
    assertTrue(databases.contains(DB1));
    assertEquals(true, groundStore.dropDatabase(DB1));
    numDBs--;

    List<String> databases2 = groundStore.getAllDatabases();
    assertFalse(databases2.contains(DB1));
    assertEquals(numDBs, databases2.size());

    Database db2v2 = new Database(DB2, "new description", "another_locationurl", new HashMap<String, String>());
    groundStore.alterDatabase(DB2, db2v2);
    Database db2v2r = groundStore.getDatabase(DB2);
    assertEquals(db2v2.getName(), db2v2r.getName());
    assertNotEquals(db2.getDescription(), db2v2r.getDescription());
    assertEquals(db2v2.getDescription(), db2v2r.getDescription());
    assertEquals("new description", db2v2r.getDescription());
    assertNotEquals(db2.getLocationUri(), db2v2r.getLocationUri());
    assertEquals(db2v2.getLocationUri(), db2v2r.getLocationUri());
    assertEquals("another_locationurl", db2v2r.getLocationUri());
  }

  /**
   * Test table operations
   */
  @Ignore
  @Test
  public void testTableOps()
      throws MetaException, InvalidObjectException, NoSuchObjectException, InvalidInputException {
    Database db1 = new Database(DBTBL1, "description", "locationurl", new HashMap<String, String>());
    groundStore.createDatabase(db1);
    StorageDescriptor sd = new StorageDescriptor(null, "location", null, null, false, 0,
        new SerDeInfo("SerDeName", "serializationLib", null), null, null, null);
    HashMap<String, String> params = new HashMap<String, String>();
    params.put("EXTERNAL", "false");
    Table tbl1 = new Table(TABLE1, DBTBL1, "owner", 1, 2, 3, sd, null, params, "viewOriginalText",
        "viewExpandedText", "MANAGED_TABLE");
    groundStore.createTable(tbl1);
    /** getAllTables TODO */
    Table table = groundStore.getTable(DBTBL1, TABLE1);
    // Assert.assertEquals(1, tables.size());
    assertEquals(TABLE1, table.getTableName());
    assertEquals(true, groundStore.dropTable(DBTBL1, TABLE1));
  }

  @Ignore
  @Test
  public void testPartitionOps()
      throws MetaException, InvalidObjectException, NoSuchObjectException, InvalidInputException {
    Database db1 = new Database(DBPART1, "description", "locationurl", new HashMap<String, String>());
    groundStore.createDatabase(db1);
    StorageDescriptor sd = new StorageDescriptor(null, "location", null, null, false, 0,
        new SerDeInfo("SerDeName", "serializationLib", null), null, null, null);
    HashMap<String, String> tableParams = new HashMap<String, String>();
    tableParams.put("EXTERNAL", "false");
    FieldSchema partitionKey1 = new FieldSchema("Country", serdeConstants.STRING_TYPE_NAME, "");
    FieldSchema partitionKey2 = new FieldSchema("State", serdeConstants.STRING_TYPE_NAME, "");
    Table tbl1 = new Table(PARTTABLE1, DBPART1, "owner", 1, 2, 3, sd, Arrays.asList(partitionKey1, partitionKey2),
        tableParams, "viewOriginalText", "viewExpandedText", "MANAGED_TABLE");
    groundStore.createTable(tbl1);
    HashMap<String, String> partitionParams = new HashMap<String, String>();
    partitionParams.put("PARTITION_LEVEL_PRIVILEGE", "true");
    List<String> value1 = Arrays.asList("US", "CA");
    Partition part1 = new Partition(value1, DBPART1, PARTTABLE1, 111, 111, sd, partitionParams);
    groundStore.addPartition(part1);
    List<String> value2 = Arrays.asList("US", "MA");
    Partition part2 = new Partition(value2, DBPART1, PARTTABLE1, 222, 222, sd, partitionParams);
    groundStore.addPartition(part2);

    Deadline.startTimer("getPartition");
    List<Partition> partitions = groundStore.getPartitions(DBPART1, PARTTABLE1, 10);
    assertEquals(2, partitions.size());
    List<Integer> timestampList = new ArrayList<Integer>();
    timestampList.add(partitions.get(0).getCreateTime());
    timestampList.add(partitions.get(1).getCreateTime());
    assertTrue(timestampList.contains(111));
    assertTrue(timestampList.contains(222));
  }

}
