/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.ground.plugins.hive;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Deadline;
import org.apache.hadoop.hive.metastore.FileFormatProxy;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.hbase.HBaseConnection;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.annotations.VisibleForTesting;

import edu.berkeley.ground.api.models.cassandra.CassandraEdgeVersionFactory;
import edu.berkeley.ground.api.models.cassandra.CassandraGraphVersionFactory;
import edu.berkeley.ground.api.models.cassandra.CassandraNodeVersionFactory;

public class TestGroundMetastore {
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
        conf.setVar(HiveConf.ConfVars.METASTORE_EXPRESSION_PROXY_CLASS, MockPartitionExpressionProxy.class.getName());
        HiveConf.setVar(conf, HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER, "test_connection");
        conf.set(GRAPHFACTORY_CLASS, CassandraGraphVersionFactory.class.getName());
        conf.set(NODEFACTORY_CLASS, CassandraNodeVersionFactory.class.getName());
        conf.set(EDGEFACTORY_CLASS, CassandraEdgeVersionFactory.class.getName());
        GroundReadWrite.setConf(conf);
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
        Database db1 = new Database(DB1, "description", "locationurl", new HashMap<String, String>());
        Database db2 = new Database(DB2, "description", "locationurl", new HashMap<String, String>());
        groundStore.createDatabase(db1);
        groundStore.createDatabase(db2);

        List<String> databases = groundStore.getAllDatabases();
        Assert.assertEquals(2, databases.size());
        // Assert.assertEquals(DB1, databases.get(0));
        // Assert.assertEquals(DB2, databases.get(1));
    }

    /**
     * Test table operations
     */
    @Test
    public void testTableOps()
            throws MetaException, InvalidObjectException, NoSuchObjectException, InvalidInputException {
        Database db1 = new Database(DBTBL1, "description", "locationurl", new HashMap<String, String>());
        groundStore.createDatabase(db1);
        StorageDescriptor sd = new StorageDescriptor(null, "location", null, null, false, 0,
                new SerDeInfo("SerDeName", "serializationLib", null), null, null, null);
        HashMap<String, String> params = new HashMap<String, String>();
        params.put("EXTERNAL", "false");
        Table tbl1 = new Table(TABLE1, DBTBL1, "owner", 1, 2, 3, sd, null, params, "viewOriginalText", "viewExpandedText",
                "MANAGED_TABLE");
        groundStore.createTable(tbl1);

        List<String> tables = groundStore.getAllTables(DBTBL1);
        Assert.assertEquals(1, tables.size());
        Assert.assertEquals(TABLE1, tables.get(0));
    }
}
