package edu.berkeley.ground;

import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.db.GremlinClient;
import edu.berkeley.ground.resources.*;
import edu.berkeley.ground.util.CassandraFactories;
import edu.berkeley.ground.util.PostgresFactories;
import edu.berkeley.ground.util.GremlinFactories;
import org.junit.Before;

import java.io.File;


public class GroundResourceTest {
    private static final String BACKING_STORE_TYPE = "postgres";
    private static final String TEST_DB_NAME = "test";

    protected NodesResource nodesResource;
    protected EdgesResource edgesResource;
    protected GraphsResource graphsResource;
    protected LineageEdgesResource lineageEdgesResource;
    protected StructuresResource structuresResource;

    @Before
    public void setUp() {
        try {
            switch (BACKING_STORE_TYPE) {
                case "postgres": {
                    setBackingStore();

                    Process p = Runtime.getRuntime().exec("python2.7 postgres_setup.py test " + TEST_DB_NAME, null, new File("scripts/postgres/"));
                    p.waitFor();

                    break;
                }

                case "cassandra": {
                    setBackingStore();

                    Process p = Runtime.getRuntime().exec("python2.7 cassandra_setup.py " + TEST_DB_NAME, null, new File("scripts/cassandra/"));
                    p.waitFor();

                    break;
                }

                case "gremlin": {
                    Process p = Runtime.getRuntime().exec("python2.7 drop_cassandra.py", null, new File("scripts/ground/"));
                    p.waitFor();

                    setBackingStore();

                    break;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("FATAL: Unexpected Exception. " + e.getMessage());
        }
    }

    private void setBackingStore() {
        switch (BACKING_STORE_TYPE) {
            case "postgres": {
                PostgresClient dbClient = new PostgresClient("localhost", 5432, "test", "test", "");
                PostgresFactories factoryGenerator = new PostgresFactories(dbClient);

                nodesResource = new NodesResource(factoryGenerator.getNodeFactory(), factoryGenerator.getNodeVersionFactory());
                edgesResource = new EdgesResource(factoryGenerator.getEdgeFactory(), factoryGenerator.getEdgeVersionFactory());
                graphsResource = new GraphsResource(factoryGenerator.getGraphFactory(), factoryGenerator.getGraphVersionFactory());
                lineageEdgesResource = new LineageEdgesResource(factoryGenerator.getLineageEdgeFactory(), factoryGenerator.getLineageEdgeVersionFactory());
                structuresResource = new StructuresResource(factoryGenerator.getStructureFactory(), factoryGenerator.getStructureVersionFactory());
                break;
            }

            case "cassandra": {
                CassandraClient dbClient = new CassandraClient("localhost", 9160, "test", "test", "");
                CassandraFactories factoryGenerator = new CassandraFactories(dbClient);

                nodesResource = new NodesResource(factoryGenerator.getNodeFactory(), factoryGenerator.getNodeVersionFactory());
                edgesResource = new EdgesResource(factoryGenerator.getEdgeFactory(), factoryGenerator.getEdgeVersionFactory());
                graphsResource = new GraphsResource(factoryGenerator.getGraphFactory(), factoryGenerator.getGraphVersionFactory());
                lineageEdgesResource = new LineageEdgesResource(factoryGenerator.getLineageEdgeFactory(), factoryGenerator.getLineageEdgeVersionFactory());
                structuresResource = new StructuresResource(factoryGenerator.getStructureFactory(), factoryGenerator.getStructureVersionFactory());
                break;
            }

            case "grermlin": {
                GremlinClient dbClient = new GremlinClient();
                GremlinFactories factoryGenerator = new GremlinFactories(dbClient);

                nodesResource = new NodesResource(factoryGenerator.getNodeFactory(), factoryGenerator.getNodeVersionFactory());
                edgesResource = new EdgesResource(factoryGenerator.getEdgeFactory(), factoryGenerator.getEdgeVersionFactory());
                graphsResource = new GraphsResource(factoryGenerator.getGraphFactory(), factoryGenerator.getGraphVersionFactory());
                lineageEdgesResource = new LineageEdgesResource(factoryGenerator.getLineageEdgeFactory(), factoryGenerator.getLineageEdgeVersionFactory());
                structuresResource = new StructuresResource(factoryGenerator.getStructureFactory(), factoryGenerator.getStructureVersionFactory());

                break;
            }

        }
    }
}
