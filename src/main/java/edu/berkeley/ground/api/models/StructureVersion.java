package edu.berkeley.ground.api.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.api.versions.Version;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.DbUtils;
import edu.berkeley.ground.util.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class StructureVersion extends Version {
    private static final Logger LOGGER = LoggerFactory.getLogger(StructureVersion.class);

    // the id of the Structure containing this Version
    private String structureId;

    // the map of attribute names to types
    private Map<String, Type> attributes;

    @JsonCreator
    protected StructureVersion(@JsonProperty("id") String id,
                               @JsonProperty("structureId") String structureId,
                               @JsonProperty("attributes") Map<String, Type> attributes) {
        super(id);

        this.structureId = structureId;
        this.attributes = attributes;
    }

    @JsonProperty
    public String getStructureId() {
        return this.structureId;
    }

    @JsonProperty
    public Map<String, Type> getAttributes() {
        return this.attributes;
    }

    /* FACTORY METHODS */
    public static StructureVersion create(GroundDBConnection connection,
                                          String structureId,
                                          Map<String, Type> attributes,
                                          Optional<String> parentId) throws GroundException {

        Structure structure = Structure.retrieveFromDatabase(connection, Structure.idToName(structureId));

        String id = IdGenerator.generateId(structureId);

        Version.insertIntoDatabase(connection, id);

        List<DbDataContainer> insertions = new ArrayList<>();
        insertions.add(new DbDataContainer("id", Type.STRING, id));
        insertions.add(new DbDataContainer("structure_id", Type.STRING, structureId));

        connection.insert("StructureVersions", insertions);

        for (String key : attributes.keySet()) {
            List<DbDataContainer> itemInsertions = new ArrayList<>();
            itemInsertions.add(new DbDataContainer("svid", Type.STRING, id));
            itemInsertions.add(new DbDataContainer("key", Type.STRING, key));
            itemInsertions.add(new DbDataContainer("type", Type.STRING, attributes.get(key).toString()));

            connection.insert("StructureVersionItems", itemInsertions);
        }

        structure.update(connection, id, parentId);

        LOGGER.info("Created structure version " + id + " in structure " + structureId + ".");

        return new StructureVersion(id, structureId, attributes);
    }

    public static StructureVersion retrieveFromDatabase(GroundDBConnection connection, String id) throws GroundException {
        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("id", Type.STRING, id));
        ResultSet resultSet = connection.equalitySelect("StructureVersions", DBClient.SELECT_STAR, predicates);

        List<DbDataContainer> attributePredicates = new ArrayList<>();
        attributePredicates.add(new DbDataContainer("svid", Type.STRING, id));
        ResultSet attributesSet = connection.equalitySelect("StructureVersionItems", DBClient.SELECT_STAR, attributePredicates);

        try {
            Map<String, Type> attributes = new HashMap<>();

            do {
                attributes.put(DbUtils.getString(attributesSet, 2), Type.fromString(DbUtils.getString(attributesSet, 3)));
            } while (attributesSet.next());

            String structureId = DbUtils.getString(resultSet, 2);

            LOGGER.info("Retrieved structure version " + id + " in structure " + structureId + ".");

            return new StructureVersion(id, structureId, attributes);
        } catch (SQLException e) {
            LOGGER.error(e.getMessage());

            throw new GroundException(e);
        }
    }
}
