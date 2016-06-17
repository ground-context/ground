package edu.berkeley.ground.api.models.titan;

import com.thinkaurelius.titan.core.TitanVertex;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.models.TagFactory;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.TitanClient;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.*;

public class TitanTagFactory extends TagFactory {
    public Optional<Map<String, Tag>> retrieveFromDatabaseById(GroundDBConnection connectionPointer, String id) throws GroundException {
        TitanClient.TitanConnection connection = (TitanClient.TitanConnection) connectionPointer;

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("id", Type.STRING, id));

        TitanVertex versionVertex = connection.getVertex(predicates);

        List<TitanVertex> tagVertices = connection.getAdjacentVerticesByEdgeLabel(versionVertex, "TagConnection");

        if(!tagVertices.isEmpty()) {
            Map<String, Tag> tags = new HashMap<>();

            for(TitanVertex tag : tagVertices) {
                String key = tag.property("tkey").value().toString();
                Optional<Object> value = Optional.ofNullable(tag.property("value").value());
                Optional<Type> type = Optional.ofNullable(Type.fromString(tag.property("type").value().toString()));

                tags.put(key, new Tag(id, key, value, type));
            }

            return Optional.of(tags);
        } else {
            return Optional.empty();
        }
    }
}
