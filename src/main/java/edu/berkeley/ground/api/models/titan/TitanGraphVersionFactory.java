package edu.berkeley.ground.api.models.titan;

import com.thinkaurelius.titan.core.TitanVertex;
import edu.berkeley.ground.api.models.GraphVersion;
import edu.berkeley.ground.api.models.GraphVersionFactory;
import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.TitanClient;
import edu.berkeley.ground.db.TitanClient.TitanConnection;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class TitanGraphVersionFactory extends GraphVersionFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(TitanGraphVersionFactory.class);
    private TitanClient dbClient;

    private TitanGraphFactory graphFactory;
    private TitanRichVersionFactory richVersionFactory;

    public TitanGraphVersionFactory(TitanGraphFactory graphFactory, TitanRichVersionFactory richVersionFactory, TitanClient dbClient) {
        this.dbClient = dbClient;
        this.graphFactory = graphFactory;
        this.richVersionFactory = richVersionFactory;
    }

    public GraphVersion create(Optional<Map<String, Tag>> tags,
                               Optional<String> structureVersionId,
                               Optional<String> reference,
                               Optional<Map<String, String>> parameters,
                               String graphId,
                               List<String> edgeVersionIds,
                               Optional<String> parentId) throws GroundException {

        TitanConnection connection = this.dbClient.getConnection();

        try {
            String id = IdGenerator.generateId(graphId);

            tags = tags.map(tagsMap ->
                                    tagsMap.values().stream().collect(Collectors.toMap(Tag::getKey, tag -> new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType())))
            );


            List<DbDataContainer> insertions = new ArrayList<>();
            insertions.add(new DbDataContainer("id", Type.STRING, id));
            insertions.add(new DbDataContainer("graph_id", Type.STRING, graphId));

            TitanVertex versionVertex = connection.addVertex("GraphVersion", insertions);
            this.richVersionFactory.insertIntoDatabase(connection, id, tags, structureVersionId, reference, parameters);

            for (String edgeVersionId : edgeVersionIds) {
                List<DbDataContainer> edgeQuery = new ArrayList<>();
                edgeQuery.add(new DbDataContainer("id", Type.STRING, edgeVersionId));
                TitanVertex edgeVertex = connection.getVertex(edgeQuery);

                connection.addEdge("GraphVersionEdge", versionVertex, edgeVertex, new ArrayList<>());
            }

            this.graphFactory.update(connection, graphId, id, parentId);

            connection.commit();
            LOGGER.info("Created graph version " + id + " in graph " + graphId + ".");

            return GraphVersionFactory.construct(id, tags, structureVersionId, reference, parameters, graphId, edgeVersionIds);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }

    public GraphVersion retrieveFromDatabase(String id) throws GroundException {
        TitanConnection connection = this.dbClient.getConnection();

        try {
            RichVersion version = this.richVersionFactory.retrieveFromDatabase(connection, id);

            List<DbDataContainer> predicates = new ArrayList<>();
            predicates.add(new DbDataContainer("id", Type.STRING, id));

            TitanVertex versionVertex = connection.getVertex(predicates);
            String graphId = versionVertex.property("graph_id").value().toString();

            List<TitanVertex> edgeVersionVertices = connection.getAdjacentVerticesByEdgeLabel(versionVertex, "GraphVersionEdge");
            List<String> edgeVersionIds = new ArrayList<>();

            for (TitanVertex edgeVersionVertex : edgeVersionVertices) {
                edgeVersionIds.add(edgeVersionVertex.property("id").value().toString());
            }

            connection.commit();
            LOGGER.info("Retrieved graph version " + id + " in graph " + graphId + ".");

            return GraphVersionFactory.construct(id, version.getTags(), version.getStructureVersionId(), version.getReference(), version.getParameters(), graphId, edgeVersionIds);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }
}
