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
package edu.berkeley.ground.postgres.utils;

import akka.actor.ActorSystem;
import com.google.common.base.CaseFormat;
import edu.berkeley.ground.common.exception.GroundException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import play.Logger;
import play.db.Database;
import play.libs.concurrent.HttpExecution;

public final class PostgresUtils {

  private PostgresUtils() {
  }

  public static Executor getDbSourceHttpContext(final ActorSystem actorSystem) {
    return HttpExecution.fromThread(
      (Executor) actorSystem.dispatchers().lookup("ground.db.context"));
  }

  public static String executeQueryToJson(Database dbSource, String sql) throws GroundException {
    // Logger.debug("executeQueryToJson sql : {}", sql);
    try {
      try (Connection con = dbSource.getConnection()) {
        try (Statement stmt = con.createStatement()) {
          try {
            final ResultSet resultSet = stmt.executeQuery(sql);
            final long columnCount = resultSet.getMetaData().getColumnCount();
            final List<Map<String, Object>> objList = new ArrayList<>();
            while (resultSet.next()) {
              final Map<String, Object> rowData = new HashMap<String, Object>();
              for (int column = 1; column <= columnCount; ++column) {
                String key = CaseFormat.LOWER_UNDERSCORE
                  .to(CaseFormat.LOWER_CAMEL, resultSet.getMetaData().getColumnLabel(column));
                rowData.put(key, resultSet.getObject(column));
              }
              objList.add(rowData);
            }
            con.close();
            return GroundUtils.listtoJson(objList);
          } catch (SQLException e) {
            Logger.error(
              "ERROR: executeQueryToJson SQL : {} Message: {} Cause {}. Trace: {}",
              sql,
              e.getMessage(),
              e.getCause(),
              e.getStackTrace());
            throw new GroundException(e);
          }
        }
      }
    } catch (SQLException e) {
      Logger.error(
        "ERROR:  executeQueryToJson  SQL : {} Message: {} Trace: {}",
        sql,
        e.getMessage(),
        e.getStackTrace());
      throw new GroundException(e);
    }
  }

  public static String executeSqlList(final Database dbSource,
    final PostgresStatements statements) throws GroundException {
    String status = null;

    try {
      try (Connection con = dbSource.getConnection()) {
        con.setAutoCommit(false);
        try (Statement stmt = con.createStatement()) {
          for (final String sql : statements.getAllStatements()) {
            try {
              // Logger.debug("executeSqlList sql : {}", sql);
              try {
                stmt.execute(sql);
              } catch (final SQLException e) {
                con.rollback();
                Logger.error("error:  Message: {} Trace: {}", e.getMessage(), e.getStackTrace());
                throw new GroundException(e);
                //throw new RuntimeException("Trying to execute sql. sql : " + sql + e.getMessage(), e);
              }
            } catch (SQLException e) {
              Logger.error(
                "error: executeSqlList SQL : {} Message: {} Cause {}. Trace: {}",
                sql,
                e.getMessage(),
                e.getCause(),
                e.getStackTrace());
              throw new GroundException(e);
            }
          }
          con.commit();
          con.close();
          status = "SUCCESS";
          return status;
        }
      }
    } catch (SQLException e) {
      Logger.error(
        "error:  executeSqlList SQL : {} Message: {} Trace: {}",
        statements.getAllStatements(),
        e.getMessage(),
        e.getStackTrace());
      throw new GroundException(e);
      //throw new RuntimeException("error :  executeSqlList." + e.getMessage(), e);
    }
  }
}
