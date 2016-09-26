package edu.berkeley.ground.plugins.hive;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
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
import edu.berkeley.ground.api.models.NodeVersionFactory;
import edu.berkeley.ground.api.models.RichVersionFactory;
import edu.berkeley.ground.api.models.StructureFactory;
import edu.berkeley.ground.api.models.StructureVersionFactory;
import edu.berkeley.ground.api.models.TagFactory;
import edu.berkeley.ground.api.models.postgres.PostgresEdgeFactory;
import edu.berkeley.ground.api.models.postgres.PostgresEdgeVersionFactory;
import edu.berkeley.ground.api.models.postgres.PostgresGraphVersionFactory;
import edu.berkeley.ground.api.models.postgres.PostgresNodeFactory;
import edu.berkeley.ground.api.models.postgres.PostgresNodeVersionFactory;
import edu.berkeley.ground.api.models.postgres.PostgresRichVersionFactory;
import edu.berkeley.ground.api.models.postgres.PostgresStructureFactory;
import edu.berkeley.ground.api.models.postgres.PostgresStructureVersionFactory;
import edu.berkeley.ground.api.models.postgres.PostgresTagFactory;
import edu.berkeley.ground.api.versions.ItemFactory;
import edu.berkeley.ground.api.versions.VersionFactory;
import edu.berkeley.ground.api.versions.postgres.PostgresItemFactory;
import edu.berkeley.ground.api.versions.postgres.PostgresVersionFactory;
import edu.berkeley.ground.api.versions.postgres.PostgresVersionHistoryDAGFactory;
import edu.berkeley.ground.api.versions.postgres.PostgresVersionSuccessorFactory;

public class GroundReadWrite {

    private static final String GROUNDCONF = "ground.properties";

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
    /** tableMap are intended ONLY for Testing will be removed.*/
    private Map<String, Map<String, ObjectPair<String, Object>>> dbTableMap =
            Collections.synchronizedMap(new HashMap<String, Map<String, ObjectPair<String, Object>>>());
    private Map<ObjectPair<String, String>, List<String>> partCache = Collections
            .synchronizedMap(new HashMap<ObjectPair<String, String>, List<String>>());

    @VisibleForTesting
    final static String TEST_CONN = "test_connection";
    private static GroundDBConnection testConn;

    private static Configuration staticConf = null;
    private GroundDBConnection conn;

    private NodeFactory nodeFactory;

    private EdgeFactory edgeFactory;

    private StructureVersionFactory svf;

    private StructureFactory sf;

    public StructureVersionFactory getStructureVersionFactory() {
        return svf;
    }

    public void setStructureVersionFactory(StructureVersionFactory svf) {
        this.svf = svf;
    }

    /**
     * @return the sf
     */
    public StructureFactory getStructureFactory() {
        return sf;
    }

    /**
     * @param sf the sf to set
     */
    public void setSFactory(StructureFactory sf) {
        this.sf = sf;
    }

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
     * Set the configuration for all GroundReadWrite instances.
     * 
     * @param configuration
     *            Configuration object
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

    private GroundReadWrite(Configuration conf) {
        Properties props = new Properties();
        try {
            String groundPropertyResource = GROUNDCONF; //ground properties from resources
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            try(InputStream resourceStream = loader.getResourceAsStream(groundPropertyResource)) {
                props.load(resourceStream);
                resourceStream.close();
            }
            //
            String clientClass = props.getProperty("edu.berkeley.ground.model.config.dbClient");
            String host = props.getProperty("edu.berkeley.ground.model.config.host");
            int port = new Integer(props.getProperty("edu.berkeley.ground.model.config.port"));
            String dbName = props.getProperty("edu.berkeley.ground.model.config.dbName");
            String userName = props.getProperty("edu.berkeley.ground.model.config.user");
            String password = props.getProperty("edu.berkeley.ground.model.config.password");
            LOG.info("client cass is " + clientClass);
            if (TEST_CONN.equals(clientClass)) {
                setConn(testConn);
                LOG.info("Using test connection."); // for unit and integration test
                dbClient = new PostgresClient("127.0.0.1", 5432, "test", "test", "test");
                createInstance();
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
                createInstance();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void createInstance() throws GroundDBException {
        PostgresVersionSuccessorFactory succ = new PostgresVersionSuccessorFactory();
        PostgresVersionHistoryDAGFactory dagFactory = new PostgresVersionHistoryDAGFactory(succ);
        PostgresItemFactory itemFactory = new PostgresItemFactory(dagFactory);
        PostgresTagFactory tagFactory = new PostgresTagFactory();
        nodeFactory = new PostgresNodeFactory(itemFactory, (PostgresClient) dbClient);
        VersionFactory vf = new PostgresVersionFactory();
        ItemFactory iff = new PostgresItemFactory(dagFactory);
        sf = new PostgresStructureFactory((PostgresItemFactory) iff, (PostgresClient) dbClient);
        svf = new PostgresStructureVersionFactory((PostgresStructureFactory) sf,
                (PostgresVersionFactory) vf, (PostgresClient) dbClient);
        RichVersionFactory rf = new PostgresRichVersionFactory((PostgresVersionFactory) vf,
                (PostgresStructureVersionFactory) svf, tagFactory);

        edgeFactory = new PostgresEdgeFactory(itemFactory, (PostgresClient) dbClient);
        edgeVersionFactory = new PostgresEdgeVersionFactory((PostgresEdgeFactory) edgeFactory,
                (PostgresRichVersionFactory) rf, (PostgresClient) dbClient);
        LOG.info("postgresclient " + dbClient.getConnection().toString());
        nodeVersionFactory = new PostgresNodeVersionFactory((PostgresNodeFactory) nodeFactory,
                (PostgresRichVersionFactory) rf, (PostgresClient) dbClient);
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

    /**
     * Add a partition. This should only be called for new partitions. For
     * altering existing partitions this should not be called as it will blindly
     * increment the ref counter for the storage descriptor.
     * 
     * @param partition
     *            partition object to add
     * @throws IOException
     */
    void putPartition(Partition partition) throws IOException {
        // TODO (krishna) use hbase model and PartitionCache to store
    }

    public Map<String, Map<String, ObjectPair<String, Object>>> getDbTableMap() {
        return dbTableMap;
    }

    public void setDbTableMap(Map<String, Map<String, ObjectPair<String, Object>>> dbTable) {
        this.dbTableMap = dbTable;
    }

    public Map<ObjectPair<String, String>, List<String>> getPartCache() {
        return partCache;
    }

    public void setPartCache(Map<ObjectPair<String, String>, List<String>> partCache) {
        this.partCache = partCache;
    }
}
