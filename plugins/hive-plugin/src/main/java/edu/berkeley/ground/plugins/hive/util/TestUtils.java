package edu.berkeley.ground.plugins.hive.util;

import edu.berkeley.ground.api.models.EdgeVersionFactory;
import edu.berkeley.ground.api.models.NodeFactory;
import edu.berkeley.ground.api.models.NodeVersionFactory;
import edu.berkeley.ground.api.models.RichVersionFactory;
import edu.berkeley.ground.api.models.StructureFactory;
import edu.berkeley.ground.api.models.StructureVersionFactory;
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
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.PostgresClient;
import edu.berkeley.ground.exceptions.GroundDBException;

public class TestUtils {

	public static void createTestInstances(DBClient dbClient, NodeVersionFactory nodeVersionFactory,
			EdgeVersionFactory edgeVersionFactory) throws GroundDBException {
    	//TODO move this
        dbClient = new PostgresClient("127.0.0.1", 5432, "default",
        		"test", "test");
        PostgresVersionSuccessorFactory succ = new PostgresVersionSuccessorFactory();
        PostgresVersionHistoryDAGFactory dagFactory = new PostgresVersionHistoryDAGFactory(succ);
        PostgresItemFactory itemFactory = new PostgresItemFactory(dagFactory);
        NodeFactory nf = new PostgresNodeFactory(itemFactory, (PostgresClient) dbClient);
        VersionFactory vf = new PostgresVersionFactory();
        ItemFactory iff = new PostgresItemFactory(null);
        StructureFactory sf = new PostgresStructureFactory((PostgresItemFactory) iff,
        		(PostgresClient) dbClient);
        StructureVersionFactory svf = new PostgresStructureVersionFactory((PostgresStructureFactory) sf,
        		(PostgresVersionFactory) vf, (PostgresClient) dbClient);
        RichVersionFactory rf = new PostgresRichVersionFactory((PostgresVersionFactory) vf, (PostgresStructureVersionFactory) svf, null);

        PostgresEdgeFactory pef = new PostgresEdgeFactory(itemFactory, (PostgresClient) dbClient);
        edgeVersionFactory = new PostgresEdgeVersionFactory(pef, 
        		(PostgresRichVersionFactory) rf, (PostgresClient) dbClient);

        nodeVersionFactory = new PostgresNodeVersionFactory((PostgresNodeFactory) nf,
        		(PostgresRichVersionFactory) rf, (PostgresClient) dbClient);
    }
}
