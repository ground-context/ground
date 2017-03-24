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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;

public class MockUtils {
  public class MockGroundStore extends GroundStore {
    Map<String, Database> databaseMap = new HashMap<String, Database>();
    Map<String, Table> tableMap = new HashMap<String, Table>();

    @Override
    public void createDatabase(Database db) throws InvalidObjectException, MetaException {
      databaseMap.put(db.getName(), db);
    }

    @Override
    public Database getDatabase(String name) throws NoSuchObjectException {
      return databaseMap.get(name);
    }

    @Override
    public void createTable(Table table) throws InvalidObjectException, MetaException {
      tableMap.put(table.getDbName() + "->" + table.getTableName(), table);
    }

    @Override
    public Table getTable(String dbName, String tableName) throws MetaException {
      return tableMap.get(dbName + "->" + tableName);
    }
  }

  private static MockUtils mockUtils = new MockUtils();

  public static GroundStore init(HiveConf conf) {
    return mockUtils.new MockGroundStore();
  }

}
