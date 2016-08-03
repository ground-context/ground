package edu.berkeley.ground.api.models.gremlin;

import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.models.TagFactory;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.GremlinClient;
import edu.berkeley.ground.exceptions.GroundException;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

public class GremlinTagFactory extends TagFactory {
    public Optional<Map<String, Tag>> retrieveFromDatabaseById(GroundDBConnection connectionPointer, String id) throws GroundException {
        GremlinClient.GremlinConnection connection = (GremlinClient.GremlinConnection) connectionPointer;

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("id", GroundType.STRING, id));

        Vertex versionVertex = connection.getVertex(predicates);

        List<Vertex> tagVertices = connection.getAdjacentVerticesByEdgeLabel(versionVertex, "TagConnection");

        if(!tagVertices.isEmpty()) {
            Map<String, Tag> tags = new HashMap<>();

            for(Vertex tag : tagVertices) {
                String key = tag.property("tkey").value().toString();
                Optional<Object> value = Optional.ofNullable(tag.property("value").value());
                Optional<GroundType> type = Optional.ofNullable(GroundType.fromString(tag.property("type").value().toString()));

                tags.put(key, new Tag(id, key, value, type));
            }

            return Optional.of(tags);
        } else {
            return Optional.empty();
        }
    }
}
