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

package edu.berkeley.ground.postgres.util;

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
    return HttpExecution.fromThread((Executor) actorSystem.dispatchers().lookup("ground.db.context"));
  }

  public static String executeQueryToJson(Database dbSource, String sql) throws GroundException {
    Logger.debug("executeQueryToJson: {}", sql);

    try {
      Connection con = dbSource.getConnection();
      Statement stmt = con.createStatement();

      final ResultSet resultSet = stmt.executeQuery(sql);
      final long columnCount = resultSet.getMetaData().getColumnCount();
      final List<Map<String, Object>> objList = new ArrayList<>();

      while (resultSet.next()) {
        final Map<String, Object> rowData = new HashMap<>();

        for (int column = 1; column <= columnCount; column++) {
          String key = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, resultSet.getMetaData().getColumnLabel(column));
          rowData.put(key, resultSet.getObject(column));
        }

        objList.add(rowData);
      }

      stmt.close();
      con.close();
      return GroundUtils.listToJson(objList);
    } catch (SQLException e) {
      Logger.error("ERROR:  executeQueryToJson  SQL : {} Message: {} Trace: {}", sql, e.getMessage(), e.getStackTrace());
      throw new GroundException(e);
    }
  }

  public static void executeSqlList(final Database dbSource, final PostgresStatements statements) throws GroundException {
    try {
      Connection con = dbSource.getConnection();
      con.setAutoCommit(false);
      Statement stmt = con.createStatement();

      for (final String sql : statements.getAllStatements()) {
        Logger.debug("executeSqlList sql : {}", sql);

        try {
          stmt.execute(sql);
        } catch (final SQLException e) {
          con.rollback();
          Logger.error("error:  Message: {} Trace: {}", e.getMessage(), e.getStackTrace());

          throw new GroundException(e);
        }
      }

      stmt.close();
      con.commit();
      con.close();
    } catch (SQLException e) {
      Logger.error("error:  executeSqlList SQL : {} Message: {} Trace: {}", statements.getAllStatements(), e.getMessage(), e.getStackTrace());

      throw new GroundException(e);
    }
  }
}
