package edu.berkeley.ground.postgres.utils;

import akka.actor.ActorSystem;
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
  private PostgresUtils() {}

  public static Executor getDbSourceHttpContext(final ActorSystem actorSystem) {
    return HttpExecution.fromThread(
        (Executor) actorSystem.dispatchers().lookup("ground.db.context"));
  }

  public static String executeQueryToJson(Database dbSource, String sql) {
    Logger.debug("executeQueryToJson sql : {}", sql);
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
                rowData.put(
                    resultSet.getMetaData().getColumnLabel(column), resultSet.getObject(column));
              }
              objList.add(rowData);
            }
            return GroundUtils.listtoJson(objList);
          } catch (SQLException e) {
            Logger.error(
                "ERROR: executeQueryToJson SQL : {} Message: {} Cause {}. Trace: {}",
                sql,
                e.getMessage(),
                e.getCause(),
                e.getStackTrace());
            throw new RuntimeException(
                String.format("ERROR : executeQueryToJson Message: %s", e.getMessage()), e);
          }
        }
      }
    } catch (SQLException e) {
      Logger.error(
          "ERROR:  executeQueryToJson  SQL : {} Message: {} Trace: {}",
          sql,
          e.getMessage(),
          e.getStackTrace());
      throw new RuntimeException(
          String.format("ERROR : executeQueryToJson Message: %s", e.getMessage()), e);
    }
  }

  public static final String executeSqlList(final Database dbSource, final List<String> sqlList) {
    String status = null;
    try {
      try (Connection con = dbSource.getConnection()) {
        con.setAutoCommit(false);
        try (Statement stmt = con.createStatement()) {
          for (final String sql : sqlList) {
            try {
              Logger.debug("executeSqlList sql : {}", sql);
              try {
                stmt.execute(sql);
              } catch (final SQLException e) {
                con.rollback();
                Logger.error("error:  Message: {} Trace: {}", e.getMessage(), e.getStackTrace());
                throw new RuntimeException(
                    "Trying to execute sql. sql : " + sql + e.getMessage(), e);
              }
            } catch (SQLException e) {
              Logger.error(
                  "error: executeSqlList SQL : {} Message: {} Cause {}. Trace: {}",
                  sql,
                  e.getMessage(),
                  e.getCause(),
                  e.getStackTrace());
              throw new RuntimeException(
                  String.format("error : executeSqlList Message: %s", e.getMessage()), e);
            }
          }
          con.commit();
          status = "SUCCESS";
          return status;
        }
      }
    } catch (SQLException e) {
      Logger.error(
          "error:  executeSqlList SQL : {} Message: {} Trace: {}",
          sqlList,
          e.getMessage(),
          e.getStackTrace());
      throw new RuntimeException("error :  executeSqlList." + e.getMessage(), e);
    }
  }
}
