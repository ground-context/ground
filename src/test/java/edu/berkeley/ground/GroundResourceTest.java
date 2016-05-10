package edu.berkeley.ground;

import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.exceptions.GroundDBException;
import edu.berkeley.ground.resources.*;
import edu.berkeley.ground.util.PostgresFactories;
import org.junit.After;
import org.junit.Before;

import java.io.File;


public class GroundResourceTest {
    private final PostgresClient dbClient = new PostgresClient("localhost", 5432, "test", "ground", "metadata");
    private PostgresFactories factoryGenerator = new PostgresFactories(dbClient);

    protected final NodesResource nodesResource = new NodesResource(factoryGenerator.getNodeFactory(), factoryGenerator.getNodeVersionFactory());
    protected final EdgesResource edgesResource = new EdgesResource(factoryGenerator.getEdgeFactory(), factoryGenerator.getEdgeVersionFactory());
    protected final GraphsResource graphsResource = new GraphsResource(factoryGenerator.getGraphFactory(), factoryGenerator.getGraphVersionFactory());
    protected final LineageEdgesResource lineageEdgesResource = new LineageEdgesResource(factoryGenerator.getLineageEdgeFactory(), factoryGenerator.getLineageEdgeVersionFactory());
    protected final StructuresResource structuresResource = new StructuresResource(factoryGenerator.getStructureFactory(), factoryGenerator.getStructureVersionFactory());


    @Before
    public void setUp() {
        try {
            Process p = Runtime.getRuntime().exec("python2.7 postgres_setup.py test", null, new File("scripts/postgres/"));
            p.waitFor();
        } catch (Exception e) {
            throw new RuntimeException("FATAL: Unexpected IOException. " + e.getMessage());
        }
    }

    @After
    public void tearDown() {
        try {
            Process p = Runtime.getRuntime().exec("python2.7 postgres_setup.py test", null, new File("scripts/postgres/"));
            p.waitFor();
        } catch (Exception e) {
            throw new RuntimeException("FATAL: Unexpected IOException. " + e.getMessage());
        }
    }
}
