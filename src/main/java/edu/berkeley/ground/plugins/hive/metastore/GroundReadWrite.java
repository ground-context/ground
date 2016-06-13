package edu.berkeley.ground.plugins.hive.metastore;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.berkeley.ground.api.models.EdgeFactory;
import edu.berkeley.ground.api.models.GraphFactory;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.exceptions.GroundDBException;
import scala.xml.factory.NodeFactory;

public class GroundReadWrite {

  static final private Logger LOG = LoggerFactory.getLogger(GroundReadWrite.class.getName());

  private static final Object TEST_CONN = null;

  private static final String GRAPHFACTORY_CLASS = null;

  private static final String NODEFACTORY_CLASS = null;

  private static final String EDGEFACTORY_CLASS = null;
  private DBClient dbClient;
  private GraphFactory graphFactory;
  private NodeFactory nodeFactory;
  private EdgeFactory edgeFactory;
  private String factoryType;
  private static GroundDBConnection testConn;

  private static Configuration staticConf = null;
  private final Configuration conf;

  private GroundDBConnection conn;
  
  private static ThreadLocal<GroundReadWrite> self = new ThreadLocal<GroundReadWrite>() {
    @Override
    protected GroundReadWrite initialValue() {
      if (staticConf == null) {
        throw new RuntimeException("Attempt to create GroundReadWrite with no "
            + "configuration set");
      }
      return new GroundReadWrite(staticConf);
    }
  };

  /**
   * Get the instance of GroundReadWrite for the current thread.
   */
  static GroundReadWrite getInstance() {
    if (staticConf == null) {
      throw new RuntimeException("Must set conf object before getting an instance");
    }
    return self.get();
  }

  private GroundReadWrite(Configuration configuration) {
    conf = configuration;
    try {
      String clientClass = HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER);
      String graphFactoryType = conf.get(GRAPHFACTORY_CLASS);
      String nodeFactoryType = conf.get(NODEFACTORY_CLASS);
      String edgeFactoryType = conf.get(EDGEFACTORY_CLASS);
      if (TEST_CONN.equals(clientClass)) {
        conn = testConn;
        LOG.debug("Using test connection.");
      } else {
        LOG.debug("Instantiating connection class " + clientClass);
        Object o = createInstance(clientClass);
        if (DBClient.class.isAssignableFrom(o.getClass())) {
          dbClient = (DBClient) o;
          conn = dbClient.getConnection();
        } else {
          throw new IOException(clientClass + " is not an instance of DBClient.");
        }
        o = createInstance(graphFactoryType);
        if (GraphFactory.class.isAssignableFrom(o.getClass())) {
          graphFactory = (GraphFactory) o;
        }
        o = createInstance(nodeFactoryType);
        if (NodeFactory.class.isAssignableFrom(o.getClass())) {
          nodeFactory = (NodeFactory<?>) o;
        }
        o = createInstance(edgeFactoryType);
        if (EdgeFactory.class.isAssignableFrom(o.getClass())) {
          edgeFactory = (EdgeFactory) o;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Object createInstance(String clientClass)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    Class<?> c = Class.forName(clientClass);
    Object o = c.newInstance();
    return o;
  }

  public static synchronized void setConf(Configuration conf) {
    // TODO configuration parameters to initialize metastore properties

  }

  public void close() throws IOException {
  }

  public void begin() {
    try {
      dbClient.getConnection().beginTransaction();
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

  public void setGraphFactory(GraphFactory graphFactory) {
    this.graphFactory = graphFactory;
  }

  public NodeFactory<?> getNodeFactory() {
    return nodeFactory;
  }

  public void setNodeFactory(NodeFactory<?> nodeFactory) {
    this.nodeFactory = nodeFactory;
  }

  public EdgeFactory getEdgeFactory() {
    return edgeFactory;
  }

  public void setEdgeFactory(EdgeFactory edgeFactory) {
    this.edgeFactory = edgeFactory;
  }

  public String getFactoryType() {
    return factoryType;
  }

  public void setFactoryType(String factoryType) {
    this.factoryType = factoryType;
  }
}
