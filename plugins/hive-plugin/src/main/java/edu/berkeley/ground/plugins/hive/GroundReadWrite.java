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

import com.google.common.annotations.VisibleForTesting;

import edu.berkeley.ground.dao.models.EdgeFactory;
import edu.berkeley.ground.dao.models.EdgeVersionFactory;
import edu.berkeley.ground.dao.models.GraphFactory;
import edu.berkeley.ground.dao.models.GraphVersionFactory;
import edu.berkeley.ground.dao.models.NodeFactory;
import edu.berkeley.ground.dao.models.NodeVersionFactory;
import edu.berkeley.ground.dao.models.StructureFactory;
import edu.berkeley.ground.dao.models.StructureVersionFactory;
import edu.berkeley.ground.dao.models.postgres.PostgresEdgeVersionFactory;
import edu.berkeley.ground.dao.models.postgres.PostgresGraphVersionFactory;
import edu.berkeley.ground.dao.models.postgres.PostgresNodeVersionFactory;
import edu.berkeley.ground.db.DbClient;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.exceptions.GroundDbException;
import edu.berkeley.ground.util.Neo4jFactories;
import edu.berkeley.ground.util.PostgresFactories;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroundReadWrite {

  private static final String GROUNDCONF = "ground.properties";

  private static final Logger LOG = LoggerFactory.getLogger(GroundReadWrite.class.getName());

  private static final String GRAPHFACTORY_CLASS = "ground.graph.factory";
  private static final String NODEFACTORY_CLASS = "ground.node.factory";
  private static final String EDGEFACTORY_CLASS = "ground.edge.factory";

  static final String NO_CACHE_CONF = "no.use.cache";
  private DbClient dbClient;
  private GraphFactory graphFactory;
  private GraphVersionFactory graphVersionFactory;
  private NodeVersionFactory nodeVersionFactory;
  private EdgeVersionFactory edgeVersionFactory;
  private String factoryType;

  @VisibleForTesting
  private static final String TEST_CONN = "test_connection";

  protected static final String DEFAULT_FACTORY = "neo4j";

  private static Configuration staticConf = null;

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

  public StructureFactory getStructureFactory() {
    return structureFactory;
  }

  private static ThreadLocal<GroundReadWrite> self = new ThreadLocal<GroundReadWrite>() {
    @Override
    protected GroundReadWrite initialValue() {
      if (staticConf == null) {
        throw new RuntimeException("Must set conf object before getting an instance");
      }
      try {
        return new GroundReadWrite(staticConf);
      } catch (GroundDbException e) {
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

  private GroundReadWrite(Configuration conf) throws GroundDbException {
    Properties props = new Properties();
    try {
      String groundPropertyResource = GROUNDCONF; //ground properties from resources
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      try (InputStream resourceStream = loader.getResourceAsStream(groundPropertyResource)) {
        props.load(resourceStream);
        resourceStream.close();
      }
      //
      final String clientClass = props.getProperty("edu.berkeley.ground.model.config.dbClient");
      host = props.getProperty("edu.berkeley.ground.model.config.host");
      int port = new Integer(props.getProperty("edu.berkeley.ground.model.config.port"));
      dbName = props.getProperty("edu.berkeley.ground.model.config.dbName");
      userName = props.getProperty("edu.berkeley.ground.model.config.user");
      password = props.getProperty("edu.berkeley.ground.model.config.password");
      LOG.info("client cass is " + clientClass);
      if (TEST_CONN.equals(clientClass)) {
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
        dbClient = (DbClient) constructor.newInstance(host, port, dbName, userName, password);
        // dbClient = new PostgresClient(host, port, dbName, userName, password);
        LOG.debug("Instantiating connection class " + clientClass);
        if (staticConf.get("factoryType", DEFAULT_FACTORY).equals("postgres")) {
          createPostgresInstance();
        } else {
          createNeo4jInstance();
        }
      }
    } catch (Exception e) {
      throw new GroundDbException(e);
    }
  }

  private void createNeo4jInstance() {
    Neo4jFactories neo4JFactories = new Neo4jFactories((Neo4jClient) dbClient, 0, 1);
    this.nodeFactory = neo4JFactories.getNodeFactory();
    this.nodeVersionFactory = neo4JFactories.getNodeVersionFactory();
    this.edgeFactory = neo4JFactories.getEdgeFactory();
    this.edgeVersionFactory = neo4JFactories.getEdgeVersionFactory();
    this.graphFactory = neo4JFactories.getGraphFactory();
    this.structureFactory = neo4JFactories.getStructureFactory();
    this.structureVersionFactory = neo4JFactories.getStructureVersionFactory();
  }

  private void createPostgresInstance() throws GroundDbException {
    PostgresFactories postgresFactories = new PostgresFactories((PostgresClient) dbClient, 0, 1);
    this.nodeFactory = postgresFactories.getNodeFactory();
    this.nodeVersionFactory = postgresFactories.getNodeVersionFactory();
    this.edgeFactory = postgresFactories.getEdgeFactory();
    this.edgeVersionFactory = postgresFactories.getEdgeVersionFactory();
    this.graphFactory = postgresFactories.getGraphFactory();
    this.structureFactory = postgresFactories.getStructureFactory();
    this.structureVersionFactory = postgresFactories.getStructureVersionFactory();
  }

  public void close() throws IOException {
  }

  public void begin() {
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

  public DbClient getDbClient() {
    return dbClient;
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
