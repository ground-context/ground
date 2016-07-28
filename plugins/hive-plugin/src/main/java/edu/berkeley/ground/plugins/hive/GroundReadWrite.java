package edu.berkeley.ground.plugins.hive;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import edu.berkeley.ground.api.models.EdgeFactory;
import edu.berkeley.ground.api.models.EdgeVersionFactory;
import edu.berkeley.ground.api.models.GraphFactory;
import edu.berkeley.ground.api.models.GraphVersionFactory;
import edu.berkeley.ground.api.models.NodeFactory;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.exceptions.GroundDBException;
import edu.berkeley.ground.plugins.hive.util.TestUtils;
import edu.berkeley.ground.api.models.NodeVersionFactory;
import edu.berkeley.ground.api.models.RichVersionFactory;
import edu.berkeley.ground.api.models.StructureFactory;
import edu.berkeley.ground.api.models.StructureVersionFactory;
import edu.berkeley.ground.api.models.TagFactory;
import edu.berkeley.ground.api.models.postgres.PostgresEdgeFactory;
import edu.berkeley.ground.api.models.postgres.PostgresEdgeVersionFactory;
import edu.berkeley.ground.api.models.postgres.PostgresNodeFactory;
import edu.berkeley.ground.api.models.postgres.PostgresNodeVersionFactory;
import edu.berkeley.ground.api.models.postgres.PostgresRichVersionFactory;
import edu.berkeley.ground.api.models.postgres.PostgresStructureFactory;
import edu.berkeley.ground.api.models.postgres.PostgresStructureVersionFactory;
import edu.berkeley.ground.api.versions.ItemFactory;
import edu.berkeley.ground.api.versions.VersionFactory;
import edu.berkeley.ground.api.versions.postgres.PostgresItemFactory;
import edu.berkeley.ground.api.versions.postgres.PostgresVersionFactory;
import edu.berkeley.ground.api.versions.postgres.PostgresVersionHistoryDAGFactory;
import edu.berkeley.ground.api.versions.postgres.PostgresVersionSuccessorFactory;

public class GroundReadWrite {

    static final private Logger LOG = LoggerFactory.getLogger(GroundReadWrite.class.getName());

    static final String GRAPHFACTORY_CLASS = "ground.graph.factory";

    static final String NODEFACTORY_CLASS = "ground.node.factory";

    static final String EDGEFACTORY_CLASS = "ground.edge.factory";

    static final String NO_CACHE_CONF = "no.use.cache";
    private DBClient dbClient;
    private GraphVersionFactory graphFactory;
    private NodeVersionFactory nodeVersionFactory;
    private EdgeVersionFactory edgeVersionFactory;
    private TagFactory tagFactory;
    private String factoryType;

    @VisibleForTesting
    final static String TEST_CONN = "test_connection";
    private static GroundDBConnection testConn;

    private static Configuration staticConf = null;
    private final Configuration conf;

    private GroundDBConnection conn;

    private NodeFactory nodeFactory;

    private EdgeFactory edgeFactory;

    private static ThreadLocal<GroundReadWrite> self = new ThreadLocal<GroundReadWrite>() {
        @Override
        protected GroundReadWrite initialValue() {
            if (staticConf == null) {
                throw new RuntimeException("Must set conf object before getting an instance");
            }
            return new GroundReadWrite(staticConf);
        }
    };

    /**
     * Set the configuration for all HBaseReadWrite instances.
     * 
     * @param configuration
     *            Configuration object
     */
    public static synchronized void setConf(Configuration configuration) {
        if (staticConf == null) {
            staticConf = configuration;
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

    private GroundReadWrite(Configuration configuration) {
        conf = configuration;
        try {
            String clientClass = HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER);
            String graphFactoryType = conf.get(GRAPHFACTORY_CLASS);
            String nodeFactoryType = conf.get(NODEFACTORY_CLASS);
            String edgeFactoryType = conf.get(EDGEFACTORY_CLASS);
            LOG.info("client cass is " + clientClass);
            if (TEST_CONN.equals(clientClass)) {
                setConn(testConn);
                LOG.info("Using test connection.");
                createTestInstances();
            } else {
                LOG.debug("Instantiating connection class " + clientClass);
                Object o = createInstance(clientClass);
                if (DBClient.class.isAssignableFrom(o.getClass())) {
                    dbClient = (DBClient) o;
                    setConn(dbClient.getConnection());
                } else {
                    throw new IOException(clientClass + " is not an instance of DBClient.");
                }
                o = createInstance(graphFactoryType);
                if (GraphVersionFactory.class.isAssignableFrom(o.getClass())) {
                    graphFactory = (GraphVersionFactory) o;
                }
                o = createInstance(nodeFactoryType);
                if (NodeVersionFactory.class.isAssignableFrom(o.getClass())) {
                    nodeVersionFactory = (NodeVersionFactory) o;
                }
                o = createInstance(edgeFactoryType);
                if (EdgeVersionFactory.class.isAssignableFrom(o.getClass())) {
                    edgeVersionFactory = (EdgeVersionFactory) o;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void createTestInstances() throws GroundDBException {
        // TODO move this
        dbClient = new PostgresClient("127.0.0.1", 5432, "default", "test", "test");
        PostgresVersionSuccessorFactory succ = new PostgresVersionSuccessorFactory();
        PostgresVersionHistoryDAGFactory dagFactory = new PostgresVersionHistoryDAGFactory(succ);
        PostgresItemFactory itemFactory = new PostgresItemFactory(dagFactory);
        nodeFactory = new PostgresNodeFactory(itemFactory, (PostgresClient) dbClient);
        VersionFactory vf = new PostgresVersionFactory();
        ItemFactory iff = new PostgresItemFactory(null);
        StructureFactory sf = new PostgresStructureFactory((PostgresItemFactory) iff, (PostgresClient) dbClient);
        StructureVersionFactory svf = new PostgresStructureVersionFactory((PostgresStructureFactory) sf,
                (PostgresVersionFactory) vf, (PostgresClient) dbClient);
        RichVersionFactory rf = new PostgresRichVersionFactory((PostgresVersionFactory) vf,
                (PostgresStructureVersionFactory) svf, null, null);

        edgeFactory = new PostgresEdgeFactory(itemFactory, (PostgresClient) dbClient);
        edgeVersionFactory = new PostgresEdgeVersionFactory((PostgresEdgeFactory) edgeFactory,
                (PostgresRichVersionFactory) rf, (PostgresClient) dbClient);
        LOG.info("postgresclient " + dbClient.getConnection().toString());
        nodeVersionFactory = new PostgresNodeVersionFactory((PostgresNodeFactory) nodeFactory, (PostgresRichVersionFactory) rf,
                (PostgresClient) dbClient);
    }

    private Object createInstance(String clientClass)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException,
            SecurityException, IllegalArgumentException, InvocationTargetException {
        if (clientClass.contains("NodeVersion")) {
            Constructor<?> cons = Class.forName(clientClass).getConstructor(NodeFactory.class, RichVersionFactory.class,
                    DBClient.class);
            // get details from conf
            return cons.newInstance(null, null, null); // TODO use conf
        }
        if (clientClass.contains("EdgeVersion")) {
            Constructor<?> cons = Class.forName(clientClass).getConstructor(EdgeVersionFactory.class, ItemFactory.class,
                    DBClient.class);
            return cons.newInstance(null, null, null); // TODO use conf
        }
        if (clientClass.contains("GraphVersion")) {
            Constructor<?> cons = Class.forName(clientClass).getConstructor(GraphFactory.class,
                    RichVersionFactory.class, DBClient.class);
            return cons.newInstance(null, null, null); // TODO use conf
        }
        return null;
    }

    /**
     * Use this for unit testing only, so that a mock connection object can be
     * passed in.
     * 
     * @param connection
     *            Mock connection objecct
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

    public GraphVersionFactory getGraphFactory() {
        return graphFactory;
    }

    public void setGraphFactory(GraphVersionFactory graphFactory) {
        this.graphFactory = graphFactory;
    }

    public NodeVersionFactory getNodeVersionFactory() {
        return nodeVersionFactory;
    }

    public void setNodeFactory(NodeVersionFactory nodeFactory) {
        this.nodeVersionFactory = nodeFactory;
    }

    public EdgeVersionFactory getEdgeVersionFactory() {
        return edgeVersionFactory;
    }

    public void setEdgeVersionFactory(EdgeVersionFactory edgeVersionFactory) {
        this.edgeVersionFactory = edgeVersionFactory;
    }

    public String getFactoryType() {
        return factoryType;
    }

    public void setFactoryType(String factoryType) {
        this.factoryType = factoryType;
    }

    public TagFactory getTagFactory() {
        return tagFactory;
    }

    public void setTagFactory(TagFactory tagFactory) {
        this.tagFactory = tagFactory;
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
}