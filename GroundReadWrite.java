package edu.berkeley.ground.plugins.hive.metastore;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.exceptions.GroundDBException;

public class GroundReadWrite {
  private DBClient client;

  public static GroundReadWrite getInstance() {
    // TODO Auto-generated method stub
    return null;
  }

  public static synchronized void setConf(Configuration conf) {
    // TODO Auto-generated method stub

  }

  public void close() throws IOException {
  }

  public void begin() {
    try {
      client.getConnection().beginTransaction();
    } catch (GroundDBException e) {
    }
  }

  public void commit() {
    try {
      client.getConnection().commit();
    } catch (GroundDBException e) {
      throw new RuntimeException(e);
    }
  }

}
