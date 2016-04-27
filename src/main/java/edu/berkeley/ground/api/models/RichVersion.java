package edu.berkeley.ground.api.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.api.versions.Version;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.DbUtils;
import org.hibernate.validator.valuehandling.UnwrapValidatedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class RichVersion extends Version {
    private static final Logger LOGGER = LoggerFactory.getLogger(RichVersion.class);

    // the map of Keys to Tags associated with this RichVersion
    private Optional<Map<String, Tag>> tags;

    @UnwrapValidatedValue
    // the optional StructureVersion associated with this RichVersion
    private Optional<String> structureVersionId;

    @UnwrapValidatedValue
    // the optional reference associated with this RichVersion
    private Optional<String> reference;

    @UnwrapValidatedValue
    // the optional parameters associated with this RichVersion if there is a reference
    private Optional<Map<String, String>> parameters;

    protected RichVersion(String id,
                          Optional<Map<String, Tag>> tags,
                          Optional<String> structureVersionId,
                          Optional<String> reference,
                          Optional<Map<String, String>> parameters) {

        super(id);

        this.tags = tags;
        this.structureVersionId = structureVersionId;
        this.reference = reference;
        this.parameters = parameters;
    }

    @JsonProperty
    public Optional<Map<String, Tag>> getTags() {
        return this.tags;
    }

    @JsonProperty
    public Optional<String> getStructureVersionId() {
        return this.structureVersionId;
    }

    @JsonProperty
    public Optional<String> getReference() {
        return this.reference;
    }

    @JsonProperty
    public Optional<Map<String, String>> getParameters() {
        return this.parameters;
    }

    /* FACTORY METHODS */
    public static void insertIntoDatabase(GroundDBConnection connection, String id, Optional<Map<String, Tag>>tags, Optional<String> structureVersionId, Optional<String> reference, Optional<Map<String, String>> parameters) throws GroundException {
        Version.insertIntoDatabase(connection, id);

        if(structureVersionId.isPresent()) {
            checkStructureTags(connection, structureVersionId.get(), tags);
        }

        List<DbDataContainer> insertions = new ArrayList<>();
        insertions.add(new DbDataContainer("id", Type.STRING, id));
        insertions.add(new DbDataContainer("structure_id", Type.STRING, structureVersionId.orElse(null)));
        insertions.add(new DbDataContainer("reference", Type.STRING, reference.orElse(null)));

        connection.insert("RichVersions", insertions);

        if (tags.isPresent()) {
            for (String key : tags.get().keySet()) {
                Tag tag = tags.get().get(key);

                List<DbDataContainer> tagInsertion = new ArrayList<>();
                tagInsertion.add(new DbDataContainer("richversion_id", Type.STRING, id));
                tagInsertion.add(new DbDataContainer("key", Type.STRING, key));
                tagInsertion.add(new DbDataContainer("value", Type.STRING, tag.getValue().map(t -> t.toString()).orElse(null)));
                tagInsertion.add(new DbDataContainer("type", Type.STRING, tag.getValueType().map(t -> t.toString()).orElse(null)));

                connection.insert("Tags", tagInsertion);
            }
        }
    }

    public static RichVersion retrieveFromDatabase(GroundDBConnection connection, String id) throws GroundException {
        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("id", Type.STRING, id));
        ResultSet resultSet = connection.equalitySelect("RichVersions", DBClient.SELECT_STAR, predicates);

        List<DbDataContainer> parameterPredicates = new ArrayList<>();
        parameterPredicates.add(new DbDataContainer("richversion_id", Type.STRING, id));
        Map<String, String> parametersMap = new HashMap<>();
        Optional<Map<String, String>> parameters;
        try {
            ResultSet parameterSet = connection.equalitySelect("RichVersionExternalParameters", DBClient.SELECT_STAR, parameterPredicates);

            do {
                parametersMap.put(DbUtils.getString(parameterSet, 2), DbUtils.getString(parameterSet, 3));
            } while (parameterSet.next());

            parameters = Optional.of(parametersMap);
        } catch (GroundException e) {
            if (e.getMessage().contains("No results found for query")) {
                parameters = Optional.empty();
            } else {
                throw e;
            }
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());

            throw new GroundException(e);
        }
        Map<String, Tag> tagsMap;
        Optional<Map<String, Tag>> tags;

        try {
            tagsMap = Tag.retrieveFromDatabaseById(connection, id);
            tags = Optional.of(tagsMap);
        } catch (GroundException e) {
            if (!e.getMessage().contains("No results found for query")) {
                throw e;
            } else {
                tags = Optional.empty();
            }
        }

        Optional<String> reference = Optional.ofNullable(DbUtils.getString(resultSet, 3));
        Optional<String> structureVersionId = Optional.ofNullable(DbUtils.getString(resultSet, 2));

        return new RichVersion(id, tags, structureVersionId, reference, parameters);
    }

    private static boolean checkStructureTags(GroundDBConnection connection, String structureVersionId, Optional<Map<String, Tag>> tags) throws GroundException {
        StructureVersion structureVersion = StructureVersion.retrieveFromDatabase(connection, structureVersionId);
        Map<String, Type> structureVersionAttributes = structureVersion.getAttributes();

        if(!tags.isPresent()) {
            return false;
        }

        Map<String, Tag> tagsMap = tags.get();

        for (String key : structureVersionAttributes.keySet()) {

            // check if such a tag exists
            if(!tagsMap.keySet().contains(key)) {
                return false;
            } else if (!tagsMap.get(key).getValueType().isPresent()) { // check that value type is specified
                return false;
            } else if (!tagsMap.get(key).getValueType().get().equals(structureVersionAttributes.get(key))) { // check that the value type is the same
                return false;
            }
        }

        return true;
    }
}
