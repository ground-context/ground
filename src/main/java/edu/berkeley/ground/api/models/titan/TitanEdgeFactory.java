package edu.berkeley.ground.api.models.titan;

import com.thinkaurelius.titan.core.TitanVertex;
import edu.berkeley.ground.api.models.Edge;
import edu.berkeley.ground.api.models.EdgeFactory;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.api.versions.titan.TitanItemFactory;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.TitanClient;
import edu.berkeley.ground.db.TitanClient.TitanConnection;
import edu.berkeley.ground.exceptions.GroundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TitanEdgeFactory extends EdgeFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(TitanEdgeFactory.class);
    private TitanClient dbClient;
    private TitanItemFactory itemFactory;

    public TitanEdgeFactory(TitanItemFactory itemFactory, TitanClient dbClient) {
        this.dbClient = dbClient;
        this.itemFactory = itemFactory;
    }

    public Edge create(String name) throws GroundException {
        TitanConnection connection = dbClient.getConnection();

        try {
            String uniqueId = "Edges." + name;

            this.itemFactory.insertIntoDatabase(connection, uniqueId);

            List<DbDataContainer> insertions = new ArrayList<>();
            insertions.add(new DbDataContainer("name", Type.STRING, name));
            insertions.add(new DbDataContainer("id", Type.STRING, uniqueId));

            connection.addVertex("GroundEdge", insertions);

            connection.commit();
            LOGGER.info("Created edge " + name + ".");
            return EdgeFactory.construct(uniqueId, name);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }

    public Edge retrieveFromDatabase(String name) throws GroundException {
        TitanConnection connection = dbClient.getConnection();

        try {
            List<DbDataContainer> predicates = new ArrayList<>();
            predicates.add(new DbDataContainer("name", Type.STRING, name));

            TitanVertex vertex = connection.getVertex(predicates);
            String id = vertex.property("id").value().toString();

            connection.commit();
            LOGGER.info("Retrieved edge " + name + ".");

            return EdgeFactory.construct(id, name);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }

    public void update(GroundDBConnection connection, String itemId, String childId, Optional<String> parent) throws GroundException {
        this.itemFactory.update(connection, itemId, childId, parent);
    }
}
