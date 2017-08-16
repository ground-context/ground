/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.cassandra.util;

import akka.actor.ActorSystem;
import com.google.common.base.CaseFormat;
import edu.berkeley.ground.common.exception.GroundException;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import edu.berkeley.ground.cassandra.util.CassandraDatabase;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import play.Logger;
import play.libs.concurrent.HttpExecution;

public final class CassandraUtils {

  private CassandraUtils() {
  }

  public static Executor getDbSourceHttpContext(final ActorSystem actorSystem) {
    return HttpExecution.fromThread((Executor) actorSystem.dispatchers().lookup("ground.db.context"));
  }

  public static String executeQueryToJson(CassandraDatabase dbSource, String cql) throws GroundException {
    Logger.debug("executeQueryToJson: {}", cql);

    final List<Map<String, Object>> objList = new ArrayList<>();
    Session session = dbSource.getSession();

    try { 
      final ResultSet resultSet = session.execute(cql);
      
      for (Row row: resultSet.all()) {
        final Map<String, Object> rowData = new HashMap<>();

        for (int column = 0; column < resultSet.getColumnDefinitions().size(); column++) {
          String key = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, resultSet.getColumnDefinitions().getName(column));
          rowData.put(key, row.getObject(column));
        }

        objList.add(rowData);
      }
    } catch (QueryExecutionException e) {
      throw new GroundException(e);
    }

    return GroundUtils.listToJson(objList);
  }

  public static void executeCqlList(final CassandraDatabase dbSource, final CassandraStatements statements) throws GroundException {
    Session session = dbSource.getSession();

    try {
      for (final String cql : statements.getAllStatements()) {
        Logger.debug("executeCqlList cql : {}", cql);
        session.execute(cql);
      }
    } catch (QueryExecutionException e) {
      Logger.error("error:  Message: {} Trace: {}", e.getMessage(), e.getStackTrace());
      throw new GroundException(e);
    }
  }
}
