package edu.berkeley.ground.api.models.postgres;

import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.models.TagFactory;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.DbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class PostgresTagFactory extends TagFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(Tag.class);

    public Map<String, Tag> retrieveFromDatabaseById(GroundDBConnection connection, String id) throws GroundException {
        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("richversion_id", Type.STRING, id));

        ResultSet resultSet = connection.equalitySelect("Tags", DBClient.SELECT_STAR, predicates);
        Map<String, Tag> result = new HashMap<>();

        try {
            do {
                String key = DbUtils.getString(resultSet, 2);
                Optional<Type> type = Optional.ofNullable(Type.fromString(DbUtils.getString(resultSet, 4)));

                String valueString = DbUtils.getString(resultSet, 3);
                Optional<Object> value = type.map(t -> Type.stringToType(valueString, t));

                result.put(key, new Tag(id, key, value, type));
            } while (resultSet.next());

            return result;
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());

            throw new GroundException(e);
        }

    }
}
