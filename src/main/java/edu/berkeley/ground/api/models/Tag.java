package edu.berkeley.ground.api.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.DbUtils;
import org.hibernate.validator.constraints.NotEmpty;
import org.hibernate.validator.valuehandling.UnwrapValidatedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class Tag {
    private static final Logger LOGGER = LoggerFactory.getLogger(Tag.class);

    private String versionId;

    @NotEmpty
    // the Key of the Tag
    private String key;

    @UnwrapValidatedValue
    // the optional Value of the Tag
    private Optional<Object> value;

    @UnwrapValidatedValue
    // the Type of the Value if it exists
    private Optional<Type> valueType;

    @JsonCreator
    public Tag(@JsonProperty("versionId") String versionId,
               @JsonProperty("key") String key,
               @JsonProperty("value") Optional<Object> value,
                @JsonProperty("type") Optional<Type> valueType) {
        this.versionId = versionId;
        this.key = key;
        this.value = value;
        this.valueType = valueType;
    }

    @JsonProperty
    public String getVersionId() {
        return this.versionId;
    }

    @JsonProperty
    public String getKey() {
        return this.key;
    }

    @JsonProperty
    public Optional<Object> getValue() {
        return this.value;
    }

    @JsonProperty
    public Optional<Type> getValueType() {
        return this.valueType;
    }

    /* FACTORY METHODS */
    public static Map<String, Tag> retrieveFromDatabaseById(GroundDBConnection connection, String id) throws GroundException {
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

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Tag)) {
            return false;
        }

        Tag that = (Tag) other;

        return this.key.equals(that.key) && this.value.equals(that.value) && this.valueType.equals(that.valueType);
    }
}
