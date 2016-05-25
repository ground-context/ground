package edu.berkeley.ground.api.usage.titan;

import com.thinkaurelius.titan.core.TitanVertex;
import edu.berkeley.ground.api.usage.LineageEdge;
import edu.berkeley.ground.api.usage.LineageEdgeFactory;
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

public class TitanLineageEdgeFactory extends LineageEdgeFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(TitanLineageEdgeFactory.class);
    private TitanClient dbClient;

    private TitanItemFactory itemFactory;

    public TitanLineageEdgeFactory(TitanItemFactory itemFactory, TitanClient dbClient) {
        this.dbClient = dbClient;
        this.itemFactory = itemFactory;
    }

    public LineageEdge create(String name) throws GroundException {
        TitanConnection connection = this.dbClient.getConnection();

        try {
            String uniqueId = "LineageEdges." + name;

            this.itemFactory.insertIntoDatabase(connection, uniqueId);

            List<DbDataContainer> insertions = new ArrayList<>();
            insertions.add(new DbDataContainer("name", Type.STRING, name));
            insertions.add(new DbDataContainer("id", Type.STRING, uniqueId));

            connection.addVertex("LineageEdges", insertions);

            connection.commit();
            LOGGER.info("Created lineage edge " + name + ".");

            return LineageEdgeFactory.construct(uniqueId, name);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }

    public LineageEdge retrieveFromDatabase(String name) throws GroundException {
        TitanConnection connection = this.dbClient.getConnection();

        try {
            List<DbDataContainer> predicates = new ArrayList<>();
            predicates.add(new DbDataContainer("name", Type.STRING, name));
            TitanVertex vertex = connection.getVertex(predicates);
            String id = vertex.property("id").value().toString();

            connection.commit();
            LOGGER.info("Retrieved lineage edge " + name + ".");

            return LineageEdgeFactory.construct(id, name);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }

    public void update(GroundDBConnection connection, String itemId, String childId, Optional<String> parent) throws GroundException {
        this.itemFactory.update(connection, itemId, childId, parent);
    }
}
