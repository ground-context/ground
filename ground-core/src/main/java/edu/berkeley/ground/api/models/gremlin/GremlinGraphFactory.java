package edu.berkeley.ground.api.models.gremlin;

import edu.berkeley.ground.api.models.Graph;
import edu.berkeley.ground.api.models.GraphFactory;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.api.versions.gremlin.GremlinItemFactory;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.GremlinClient;
import edu.berkeley.ground.db.GremlinClient.GremlinConnection;
import edu.berkeley.ground.exceptions.GroundException;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class GremlinGraphFactory extends GraphFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(GremlinGraphFactory.class);
    private GremlinClient dbClient;

    private GremlinItemFactory itemFactory;

    public GremlinGraphFactory(GremlinItemFactory itemFactory, GremlinClient dbClient) {
        this.dbClient = dbClient;
        this.itemFactory = itemFactory;
    }

    public Graph create(String name) throws GroundException {
        GremlinConnection connection = this.dbClient.getConnection();

        try {
            String uniqueId = "Graphs." + name;
            this.itemFactory.insertIntoDatabase(connection, uniqueId);

            List<DbDataContainer> insertions = new ArrayList<>();
            insertions.add(new DbDataContainer("name", Type.STRING, name));
            insertions.add(new DbDataContainer("id", Type.STRING, uniqueId));

            connection.addVertex("Graph", insertions);

            connection.commit();
            LOGGER.info("Created graph " + name + ".");

            return GraphFactory.construct(uniqueId, name);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }

    public Graph retrieveFromDatabase(String name) throws GroundException {
        GremlinConnection connection = this.dbClient.getConnection();

        try {
            List<DbDataContainer> predicates = new ArrayList<>();
            predicates.add(new DbDataContainer("name", Type.STRING, name));

            Vertex vertex = connection.getVertex(predicates);
            String id = vertex.property("id").value().toString();

            connection.commit();
            LOGGER.info("Retrieved graph " + name + ".");

            return GraphFactory.construct(id, name);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }

    public void update(GroundDBConnection connection, String itemId, String childId, Optional<String> parent) throws GroundException {
        this.itemFactory.update(connection, "Graphs." + itemId, childId, parent);
    }
}
