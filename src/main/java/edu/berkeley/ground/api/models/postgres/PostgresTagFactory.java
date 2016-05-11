package edu.berkeley.ground.api.models.postgres;

import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.models.TagFactory;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.QueryResults;
import edu.berkeley.ground.exceptions.GroundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class PostgresTagFactory extends TagFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(Tag.class);

    public Map<String, Tag> retrieveFromDatabaseById(GroundDBConnection connection, String id) throws GroundException {
        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("richversion_id", Type.STRING, id));

        QueryResults resultSet = connection.equalitySelect("Tags", DBClient.SELECT_STAR, predicates);
        Map<String, Tag> result = new HashMap<>();

        do {
            String key = resultSet.getString(2);
            Optional<Type> type = Optional.ofNullable(Type.fromString(resultSet.getString(4)));

            String valueString = resultSet.getString(3);
            Optional<Object> value = type.map(t -> Type.stringToType(valueString, t));

            result.put(key, new Tag(id, key, value, type));
        } while (resultSet.next());

        return result;
    }
}
