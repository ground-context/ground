package edu.berkeley.ground.api.usage.titan;

import com.thinkaurelius.titan.core.TitanVertex;
import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.models.titan.TitanRichVersionFactory;
import edu.berkeley.ground.api.usage.LineageEdgeVersion;
import edu.berkeley.ground.api.usage.LineageEdgeVersionFactory;
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

public class TitanLineageEdgeVersionFactory extends LineageEdgeVersionFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(TitanLineageEdgeVersionFactory.class);
    private TitanClient dbClient;

    private TitanLineageEdgeFactory lineageEdgeFactory;
    private TitanRichVersionFactory richVersionFactory;

    public TitanLineageEdgeVersionFactory(TitanLineageEdgeFactory lineageEdgeFactory, TitanRichVersionFactory richVersionFactory, TitanClient dbClient) {
        this.dbClient = dbClient;
        this.lineageEdgeFactory = lineageEdgeFactory;
        this.richVersionFactory = richVersionFactory;
    }


    public LineageEdgeVersion create(Optional<Map<String, Tag>> tags,
                                     Optional<String> structureVersionId,
                                     Optional<String> reference,
                                     Optional<Map<String, String>> parameters,
                                     String fromId,
                                     String toId,
                                     String lineageEdgeId,
                                     Optional<String> parentId) throws GroundException {

        TitanConnection connection = this.dbClient.getConnection();

        try {
            String id = IdGenerator.generateId(lineageEdgeId);

            tags = tags.map(tagsMap ->
                                    tagsMap.values().stream().collect(Collectors.toMap(Tag::getKey, tag -> new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType())))
            );

            List<DbDataContainer> insertions = new ArrayList<>();
            insertions.add(new DbDataContainer("id", Type.STRING, id));
            insertions.add(new DbDataContainer("lineageedge_id", Type.STRING, lineageEdgeId));
            insertions.add(new DbDataContainer("endpoint_one", Type.STRING, fromId));
            insertions.add(new DbDataContainer("endpoint_two", Type.STRING, toId));

            TitanVertex versionVertex = connection.addVertex("LineageEdgeVersions", insertions);

            this.lineageEdgeFactory.update(connection, lineageEdgeId, id, parentId);
            this.richVersionFactory.insertIntoDatabase(connection, id, tags, structureVersionId, reference, parameters);

            List<DbDataContainer> predicates = new ArrayList<>();
            predicates.add(new DbDataContainer("id", Type.STRING, fromId));
            TitanVertex fromVertex = connection.getVertex(predicates);

            predicates.clear();
            predicates.add(new DbDataContainer("id", Type.STRING, toId));
            TitanVertex toVertex = connection.getVertex(predicates);

            predicates.clear();
            connection.addEdge("LineageEdgeVersionConnection", fromVertex, versionVertex, predicates);
            connection.addEdge("LineageEdgeVersionConnection", versionVertex, toVertex, predicates);

            connection.commit();
            LOGGER.info("Created lineage edge version " + id + " in lineage edge " + lineageEdgeId + ".");

            return LineageEdgeVersionFactory.construct(id, tags, structureVersionId, reference, parameters, fromId, toId, lineageEdgeId);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }

    public LineageEdgeVersion retrieveFromDatabase(String id) throws GroundException {
        TitanConnection connection = this.dbClient.getConnection();

        try {
            RichVersion version = this.richVersionFactory.retrieveFromDatabase(connection, id);

            List<DbDataContainer> predicates = new ArrayList<>();
            predicates.add(new DbDataContainer("id", Type.STRING, id));

            TitanVertex versionVertex = connection.getVertex(predicates);
            String lineageEdgeId = versionVertex.property("lineageedge_id").value().toString();
            String fromId = versionVertex.property("endpoint_one").value().toString();
            String toId = versionVertex.property("endpoint_two").value().toString();

            connection.commit();
            LOGGER.info("Retrieved lineage edge version " + id + " in lineage edge " + lineageEdgeId + ".");

            return LineageEdgeVersionFactory.construct(id, version.getTags(), version.getStructureVersionId(), version.getReference(), version.getParameters(), fromId, toId, lineageEdgeId);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }
}
