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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.AfterClass;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class TestGroundStore {
  private static final Logger LOG = LoggerFactory.getLogger(TestGroundStore.class.getName());
  static Map<String, String> emptyParameters = new HashMap<String, String>();
  // Table with NUM_PART_KEYS partitioning keys and NUM_PARTITIONS values per
  // key
  static final int NUM_PART_KEYS = 1;
  static final int NUM_PARTITIONS = 5;
  static final String DB = "db";
  static final String TBL = "tbl";
  static final String COL = "col";
  static final String PART_KEY_PREFIX = "part";
  static final String PART_VAL_PREFIX = "val";
  static final String PART_KV_SEPARATOR = "=";
  static final List<String> PART_KEYS = new ArrayList<String>();
  static final List<String> PART_VALS = new ArrayList<String>();

  // Initialize mock partitions
  static {
    for (int i = 1; i <= NUM_PART_KEYS; i++) {
      PART_KEYS.add(PART_KEY_PREFIX + i);
    }
    for (int i = 1; i <= NUM_PARTITIONS; i++) {
      PART_VALS.add(PART_VAL_PREFIX + i);
    }
  }

  static final long DEFAULT_TIME = System.currentTimeMillis();
  static final String PART_KEY = "part";
  static final String BOOLEAN_COL = "boolCol";
  static final String BOOLEAN_TYPE = "boolean";
  static final String BOOLEAN_VAL = "true";
  static final String LONG_COL = "longCol";
  static final String LONG_TYPE = "long";
  static final String INT_TYPE = "int";
  static final String INT_VAL = "1234";
  static final String DOUBLE_COL = "doubleCol";
  static final String DOUBLE_TYPE = "double";
  static final String DOUBLE_VAL = "3.1415";
  static final String STRING_COL = "stringCol";
  static final String STRING_TYPE = "string";
  static final String STRING_VAL = "stringval";
  static final String BINARY_COL = "binaryCol";
  static final String BINARY_TYPE = "binary";
  static final String BINARY_VAL = "1";
  static final String DECIMAL_COL = "decimalCol";
  static final String DECIMAL_TYPE = "decimal(5,3)";
  static final String DECIMAL_VAL = "12.123";
  static List<ColumnStatisticsObj> booleanColStatsObjs = new ArrayList<ColumnStatisticsObj>(NUM_PARTITIONS);
  static List<ColumnStatisticsObj> longColStatsObjs = new ArrayList<ColumnStatisticsObj>(NUM_PARTITIONS);
  static List<ColumnStatisticsObj> doubleColStatsObjs = new ArrayList<ColumnStatisticsObj>(NUM_PARTITIONS);
  static List<ColumnStatisticsObj> stringColStatsObjs = new ArrayList<ColumnStatisticsObj>(NUM_PARTITIONS);
  static List<ColumnStatisticsObj> binaryColStatsObjs = new ArrayList<ColumnStatisticsObj>(NUM_PARTITIONS);
  static List<ColumnStatisticsObj> decimalColStatsObjs = new ArrayList<ColumnStatisticsObj>(NUM_PARTITIONS);

  @Rule
  public ExpectedException thrown = ExpectedException.none();
  GroundStore store;

  @BeforeClass
  public static void beforeTest() {
    // All data intitializations
    populateMockStats();
  }

  private static void populateMockStats() {
    ColumnStatisticsObj statsObj;
    // Add NUM_PARTITIONS ColumnStatisticsObj of each type
    // For aggregate stats test, we'll treat each ColumnStatisticsObj as
    // stats for 1 partition
    // For the rest, we'll just pick the 1st ColumnStatisticsObj from this
    // list and use it
    for (int i = 0; i < NUM_PARTITIONS; i++) {
      statsObj = mockBooleanStats(i);
      booleanColStatsObjs.add(statsObj);
      statsObj = mockLongStats(i);
      longColStatsObjs.add(statsObj);
      statsObj = mockDoubleStats(i);
      doubleColStatsObjs.add(statsObj);
      statsObj = mockStringStats(i);
      stringColStatsObjs.add(statsObj);
      statsObj = mockBinaryStats(i);
      binaryColStatsObjs.add(statsObj);
      statsObj = mockDecimalStats(i);
      decimalColStatsObjs.add(statsObj);
    }
  }

  private static ColumnStatisticsObj mockBooleanStats(int i) {
    long trues = 37 + 100 * i;
    long falses = 12 + 50 * i;
    long nulls = 2 + i;
    ColumnStatisticsObj colStatsObj = new ColumnStatisticsObj();
    colStatsObj.setColName(BOOLEAN_COL);
    colStatsObj.setColType(BOOLEAN_TYPE);
    ColumnStatisticsData data = new ColumnStatisticsData();
    BooleanColumnStatsData boolData = new BooleanColumnStatsData();
    boolData.setNumTrues(trues);
    boolData.setNumFalses(falses);
    boolData.setNumNulls(nulls);
    data.setBooleanStats(boolData);
    colStatsObj.setStatsData(data);
    return colStatsObj;
  }

  private static ColumnStatisticsObj mockLongStats(int i) {
    long high = 120938479124L + 100 * i;
    long low = -12341243213412124L - 50 * i;
    long nulls = 23 + i;
    long dVs = 213L + 10 * i;
    ColumnStatisticsObj colStatsObj = new ColumnStatisticsObj();
    colStatsObj.setColName(LONG_COL);
    colStatsObj.setColType(LONG_TYPE);
    ColumnStatisticsData data = new ColumnStatisticsData();
    LongColumnStatsData longData = new LongColumnStatsData();
    longData.setHighValue(high);
    longData.setLowValue(low);
    longData.setNumNulls(nulls);
    longData.setNumDVs(dVs);
    data.setLongStats(longData);
    colStatsObj.setStatsData(data);
    return colStatsObj;
  }

  private static ColumnStatisticsObj mockDoubleStats(int i) {
    double high = 123423.23423 + 100 * i;
    double low = 0.00001234233 - 50 * i;
    long nulls = 92 + i;
    long dVs = 1234123421L + 10 * i;
    ColumnStatisticsObj colStatsObj = new ColumnStatisticsObj();
    colStatsObj.setColName(DOUBLE_COL);
    colStatsObj.setColType(DOUBLE_TYPE);
    ColumnStatisticsData data = new ColumnStatisticsData();
    DoubleColumnStatsData doubleData = new DoubleColumnStatsData();
    doubleData.setHighValue(high);
    doubleData.setLowValue(low);
    doubleData.setNumNulls(nulls);
    doubleData.setNumDVs(dVs);
    data.setDoubleStats(doubleData);
    colStatsObj.setStatsData(data);
    return colStatsObj;
  }

  private static ColumnStatisticsObj mockStringStats(int i) {
    long maxLen = 1234 + 10 * i;
    double avgLen = 32.3 + i;
    long nulls = 987 + 10 * i;
    long dVs = 906 + i;
    ColumnStatisticsObj colStatsObj = new ColumnStatisticsObj();
    colStatsObj.setColName(STRING_COL);
    colStatsObj.setColType(STRING_TYPE);
    ColumnStatisticsData data = new ColumnStatisticsData();
    StringColumnStatsData stringData = new StringColumnStatsData();
    stringData.setMaxColLen(maxLen);
    stringData.setAvgColLen(avgLen);
    stringData.setNumNulls(nulls);
    stringData.setNumDVs(dVs);
    data.setStringStats(stringData);
    colStatsObj.setStatsData(data);
    return colStatsObj;
  }

  private static ColumnStatisticsObj mockBinaryStats(int i) {
    long maxLen = 123412987L + 10 * i;
    double avgLen = 76.98 + i;
    long nulls = 976998797L + 10 * i;
    ColumnStatisticsObj colStatsObj = new ColumnStatisticsObj();
    colStatsObj.setColName(BINARY_COL);
    colStatsObj.setColType(BINARY_TYPE);
    ColumnStatisticsData data = new ColumnStatisticsData();
    BinaryColumnStatsData binaryData = new BinaryColumnStatsData();
    binaryData.setMaxColLen(maxLen);
    binaryData.setAvgColLen(avgLen);
    binaryData.setNumNulls(nulls);
    data.setBinaryStats(binaryData);
    colStatsObj.setStatsData(data);
    return colStatsObj;
  }

  private static ColumnStatisticsObj mockDecimalStats(int i) {
    Decimal high = new Decimal();
    high.setScale((short) 3);
    String strHigh = String.valueOf(3876 + 100 * i);
    high.setUnscaled(strHigh.getBytes());
    Decimal low = new Decimal();
    low.setScale((short) 3);
    String strLow = String.valueOf(38 + i);
    low.setUnscaled(strLow.getBytes());
    long nulls = 13 + i;
    long dVs = 923947293L + 100 * i;
    ColumnStatisticsObj colStatsObj = new ColumnStatisticsObj();
    colStatsObj.setColName(DECIMAL_COL);
    colStatsObj.setColType(DECIMAL_TYPE);
    ColumnStatisticsData data = new ColumnStatisticsData();
    DecimalColumnStatsData decimalData = new DecimalColumnStatsData();
    decimalData.setHighValue(high);
    decimalData.setLowValue(low);
    decimalData.setNumNulls(nulls);
    decimalData.setNumDVs(dVs);
    data.setDecimalStats(decimalData);
    colStatsObj.setStatsData(data);
    return colStatsObj;
  }

  @AfterClass
  public static void afterTest() {
  }

  @Before
  public void init() throws IOException {
    MockitoAnnotations.initMocks(this);
    HiveConf conf = new HiveConf();
    conf.setBoolean(GroundReadWrite.NO_CACHE_CONF, true);
    store = MockUtils.init(conf);
  }

  @Test
  public void createDb() throws Exception {
    String dbname = "mydb";
    Database db = new Database(dbname, "no description", "file:///tmp", emptyParameters);
    store.createDatabase(db);

    Database d = store.getDatabase(dbname);
    assertEquals(dbname, d.getName());
    assertEquals("no description", d.getDescription());
    assertEquals("file:///tmp", d.getLocationUri());
  }

  @Test
  public void createTable() throws Exception {
    String tableName = "mytable";
    int startTime = (int) (System.currentTimeMillis() / 1000);
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", "int", ""));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    Map<String, String> params = new HashMap<String, String>();
    params.put("key", "value");
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 17, serde,
        Arrays.asList("bucketcol"), Arrays.asList(new Order("sortcol", 1)), params);
    Table table = new Table(tableName, "default", "me", startTime, startTime, 0, sd, null, emptyParameters, null,
        null, null);
    store.createTable(table);

    Table t = store.getTable("default", tableName);
    assertEquals(1, t.getSd().getColsSize());
    assertEquals("col1", t.getSd().getCols().get(0).getName());
    assertEquals("int", t.getSd().getCols().get(0).getType());
    assertEquals("", t.getSd().getCols().get(0).getComment());
    assertEquals("serde", t.getSd().getSerdeInfo().getName());
    assertEquals("seriallib", t.getSd().getSerdeInfo().getSerializationLib());
    assertEquals("file:/tmp", t.getSd().getLocation());
    assertEquals("input", t.getSd().getInputFormat());
    assertEquals("output", t.getSd().getOutputFormat());
    assertFalse(t.getSd().isCompressed());
    assertEquals(17, t.getSd().getNumBuckets());
    assertEquals(1, t.getSd().getBucketColsSize());
    assertEquals("bucketcol", t.getSd().getBucketCols().get(0));
    assertEquals(1, t.getSd().getSortColsSize());
    assertEquals("sortcol", t.getSd().getSortCols().get(0).getCol());
    assertEquals(1, t.getSd().getSortCols().get(0).getOrder());
    assertEquals(1, t.getSd().getParametersSize());
    assertEquals("value", t.getSd().getParameters().get("key"));
    assertEquals("me", t.getOwner());
    assertEquals("default", t.getDbName());
    assertEquals(tableName, t.getTableName());
    assertEquals(0, t.getParametersSize());
  }

  private Table createMockTableAndPartition(String partType, String partVal) throws Exception {
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", partType, ""));
    List<String> vals = new ArrayList<String>();
    vals.add(partVal);
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    Map<String, String> params = new HashMap<String, String>();
    params.put("key", "value");
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 17, serde,
        Arrays.asList("bucketcol"), Arrays.asList(new Order("sortcol", 1)), params);
    int currentTime = (int) (System.currentTimeMillis() / 1000);
    Table table = new Table(TBL, DB, "me", currentTime, currentTime, 0, sd, cols, emptyParameters, null, null,
        null);
    store.createTable(table);
    Partition part = new Partition(vals, DB, TBL, currentTime, currentTime, sd, emptyParameters);
    store.addPartition(part);
    return table;
  }

  private Table createMockTable(String type) throws Exception {
    List<FieldSchema> cols = new ArrayList<FieldSchema>();
    cols.add(new FieldSchema("col1", type, ""));
    SerDeInfo serde = new SerDeInfo("serde", "seriallib", null);
    Map<String, String> params = new HashMap<String, String>();
    params.put("key", "value");
    StorageDescriptor sd = new StorageDescriptor(cols, "file:/tmp", "input", "output", false, 17, serde,
        Arrays.asList("bucketcol"), Arrays.asList(new Order("sortcol", 1)), params);
    int currentTime = (int) (System.currentTimeMillis() / 1000);
    Table table = new Table(TBL, DB, "me", currentTime, currentTime, 0, sd, cols, emptyParameters, null, null,
        null);
    store.createTable(table);
    return table;
  }

}
