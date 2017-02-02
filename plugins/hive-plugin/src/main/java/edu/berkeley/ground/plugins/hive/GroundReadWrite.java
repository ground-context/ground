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

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.Properties;

import com.google.common.annotations.VisibleForTesting;

import edu.berkeley.ground.api.models.*;
import edu.berkeley.ground.api.models.postgres.*;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.exceptions.GroundDBException;
import edu.berkeley.ground.util.Neo4jFactories;
import edu.berkeley.ground.util.PostgresFactories;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroundReadWrite {

  private static final String GROUNDCONF = "ground.properties";

  static final private Logger LOG = LoggerFactory.getLogger(GroundReadWrite.class.getName());

  static final String GRAPHFACTORY_CLASS = "ground.graph.factory";

  static final String NODEFACTORY_CLASS = "ground.node.factory";

  static final String EDGEFACTORY_CLASS = "ground.edge.factory";

  static final String NO_CACHE_CONF = "no.use.cache";
  private DBClient dbClient;
  private GraphFactory graphFactory;
  private GraphVersionFactory graphVersionFactory;
  private NodeVersionFactory nodeVersionFactory;
  private EdgeVersionFactory edgeVersionFactory;
  private String factoryType;

  @VisibleForTesting
  final static String TEST_CONN = "test_connection";

  protected static final String DEFAULT_FACTORY = "neo4j";
  private static GroundDBConnection testConn;

  private static Configuration staticConf = null;
  private GroundDBConnection conn;

  private NodeFactory nodeFactory;

  private EdgeFactory edgeFactory;

  private StructureVersionFactory structureVersionFactory;

  private StructureFactory structureFactory;

  private String dbName;

  private String userName;

  private String password;

  private String host;

  public StructureVersionFactory getStructureVersionFactory() {
    return structureVersionFactory;
  }

  public void setStructureVersionFactory(StructureVersionFactory svf) {
    this.structureVersionFactory = svf;
  }

  /**
   * @return the sf
   */
  public StructureFactory getStructureFactory() {
    return structureFactory;
  }

  /**
   * @param sf the sf to set
   */
  public void setStructureFactory(StructureFactory sf) {
    this.structureFactory = sf;
  }

  private static ThreadLocal<GroundReadWrite> self = new ThreadLocal<GroundReadWrite>() {
    @Override
    protected GroundReadWrite initialValue() {
      if (staticConf == null) {
        throw new RuntimeException("Must set conf object before getting an instance");
      }
      try {
        return new GroundReadWrite(staticConf);
      } catch (GroundDBException e) {
        LOG.error("create groundreadwrite failed {}", e.getMessage());
      }
      return null;
    }
  };

  /**
   * Set the configuration for all GroundReadWrite instances.
   *
   * @param conf the configuration object
   */
  public static synchronized void setConf(Configuration conf) {
    /** TODO(krishna) Need to change - temporarily using test connection
     * for hive command line as well as test.
     */
    if (staticConf == null) {
      staticConf = conf;
      //conf.setVar(HiveConf.ConfVars.METASTORE_EXPRESSION_PROXY_CLASS, MockPartitionExpressionProxy.class.getName());
      HiveConf.setVar(conf, HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER, "test_connection");
      conf.set(GRAPHFACTORY_CLASS, PostgresGraphVersionFactory.class.getName());
      conf.set(NODEFACTORY_CLASS, PostgresNodeVersionFactory.class.getName());
      conf.set(EDGEFACTORY_CLASS, PostgresEdgeVersionFactory.class.getName());
    } else {
      LOG.info("Attempt to set conf when it has already been set.");
    }
  }

  /**
   * Get the instance of GroundReadWrite for the current thread.
   */
  static GroundReadWrite getInstance() {
    if (staticConf == null) {
      throw new RuntimeException("Must set conf object before getting an instance");
    }
    return self.get();
  }

  private GroundReadWrite(Configuration conf) throws GroundDBException {
    Properties props = new Properties();
    try {
      String groundPropertyResource = GROUNDCONF; //ground properties from resources
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      try (InputStream resourceStream = loader.getResourceAsStream(groundPropertyResource)) {
        props.load(resourceStream);
        resourceStream.close();
      }
      //
      String clientClass = props.getProperty("edu.berkeley.ground.model.config.dbClient");
      host = props.getProperty("edu.berkeley.ground.model.config.host");
      int port = new Integer(props.getProperty("edu.berkeley.ground.model.config.port"));
      dbName = props.getProperty("edu.berkeley.ground.model.config.dbName");
      userName = props.getProperty("edu.berkeley.ground.model.config.user");
      password = props.getProperty("edu.berkeley.ground.model.config.password");
      LOG.info("client cass is " + clientClass);
      if (TEST_CONN.equals(clientClass)) {
        setConn(testConn);
        factoryType = props.getProperty("edu.berkeley.ground.model.config.factoryType");
        LOG.info("Using test connection."); // for unit and integration test
        if (factoryType.contains("neo4j")) {
          dbClient = new Neo4jClient(host, userName, password);
          createNeo4jInstance();
        } else {
          dbClient = new PostgresClient("127.0.0.1", 5432, "test", "test", "test");
          createPostgresInstance();
        }
      } else {
        conf.set("edu.berkeley.ground.model.config.clientClass", clientClass);
        conf.set("edu.berkeley.ground.model.config.host", host);
        conf.set("edu.berkeley.ground.model.config.dbName", dbName);
        conf.set("edu.berkeley.ground.model.config.userName", userName);
        conf.set("edu.berkeley.ground.model.config.password", password);
        conf.setInt("edu.berkeley.ground.model.config.port", port);
        Class<?> clazz = Class.forName(clientClass);
        Constructor<?> constructor = clazz.getConstructor(String.class, Integer.class,
            String.class, String.class, String.class);
        dbClient = (DBClient) constructor.newInstance(host, port, dbName, userName, password);
        // dbClient = new PostgresClient(host, port, dbName, userName, password);
        LOG.debug("Instantiating connection class " + clientClass);
        if (staticConf.get("factoryType", DEFAULT_FACTORY).equals("postgres")) {
          createPostgresInstance();
        } else {
          createNeo4jInstance();
        }
      }
    } catch (Exception e) {
      throw new GroundDBException(e);
    }
  }

  private void createNeo4jInstance() {
    Neo4jFactories neo4JFactories = new Neo4jFactories((Neo4jClient) dbClient);
    this.nodeFactory = neo4JFactories.getNodeFactory();
    this.nodeVersionFactory = neo4JFactories.getNodeVersionFactory();
    this.edgeFactory = neo4JFactories.getEdgeFactory();
    this.edgeVersionFactory = neo4JFactories.getEdgeVersionFactory();
    this.graphFactory = neo4JFactories.getGraphFactory();
    this.structureFactory = neo4JFactories.getStructureFactory();
    this.structureVersionFactory = neo4JFactories.getStructureVersionFactory();
  }

  private void createPostgresInstance() throws GroundDBException {
    PostgresFactories postgresFactories = new PostgresFactories((PostgresClient) dbClient);
    this.nodeFactory = postgresFactories.getNodeFactory();
    this.nodeVersionFactory = postgresFactories.getNodeVersionFactory();
    this.edgeFactory = postgresFactories.getEdgeFactory();
    this.edgeVersionFactory = postgresFactories.getEdgeVersionFactory();
    this.graphFactory = postgresFactories.getGraphFactory();
    this.structureFactory = postgresFactories.getStructureFactory();
    this.structureVersionFactory = postgresFactories.getStructureVersionFactory();
  }

  /**
   * Use this for unit testing only, so that a mock connection object can be
   * passed in.
   *
   * @param connection Mock connection objecct
   */
  @VisibleForTesting
  static void setTestConnection(GroundDBConnection connection) {
    testConn = connection;
  }

  public void close() throws IOException {
  }

  public void begin() {
    try {
      dbClient.getConnection();
    } catch (GroundDBException e) {
    }
  }

  public void commit() {
    try {
      dbClient.getConnection().commit();
    } catch (GroundDBException e) {
      throw new RuntimeException(e);
    }
  }

  public GraphFactory getGraphFactory() {
    return graphFactory;
  }

  public NodeVersionFactory getNodeVersionFactory() {
    return nodeVersionFactory;
  }

  public EdgeVersionFactory getEdgeVersionFactory() {
    return edgeVersionFactory;
  }

  public String getFactoryType() {
    return factoryType;
  }

  public GroundDBConnection getConn() {
    return conn;
  }

  public void setConn(GroundDBConnection conn) {
    this.conn = conn;
  }

  public NodeFactory getNodeFactory() {
    return nodeFactory;
  }

  public EdgeFactory getEdgeFactory() {
    return edgeFactory;
  }

  public GraphVersionFactory getGraphVersionFactory() {
    return graphVersionFactory;
  }

}
