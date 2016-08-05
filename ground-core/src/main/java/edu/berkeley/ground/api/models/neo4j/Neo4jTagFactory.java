package edu.berkeley.ground.api.models.neo4j;

import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.models.TagFactory;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.db.Neo4jClient.Neo4jConnection;
import edu.berkeley.ground.exceptions.GroundException;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.v1.Record;

import java.util.*;

public class Neo4jTagFactory extends TagFactory {
    public Optional<Map<String, Tag>> retrieveFromDatabaseById(GroundDBConnection connectionPointer, String id) throws GroundException {
        Neo4jConnection connection = (Neo4jConnection) connectionPointer;

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("id", GroundType.STRING, id));
        List<String> returnFields = new ArrayList<>();
        returnFields.add("tkey");
        returnFields.add("value");
        returnFields.add("type");

        List<Record> tagsRecords = connection.getAdjacentVerticesByEdgeLabel("TagConnection", id, returnFields);

        Map<String, Tag> tags = new HashMap<>();

        if(tagsRecords.isEmpty()) {
            return Optional.empty();
        }

        for (Record record : tagsRecords) {
            String key = Neo4jClient.getStringFromValue((StringValue) record.get("tkey"));

            Optional<Object> value;
            if (record.containsKey("value")) {
                value = Optional.of(Neo4jClient.getStringFromValue((StringValue) record.get("value")));
            } else {
                value = Optional.empty();
            }
            Optional<GroundType> type;
            if (record.containsKey("type")) {
                 type = Optional.of(GroundType.fromString(Neo4jClient.getStringFromValue((StringValue) record.get("type"))));
            } else {
                type = Optional.empty();
            }

            tags.put(key, new Tag(id, key, value, type));
        }

        return Optional.of(tags);
    }
}
