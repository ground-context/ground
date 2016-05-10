package edu.berkeley.ground.api.models.postgres;

import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.RichVersionFactory;
import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.api.versions.postgres.PostgresVersionFactory;
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

public class PostgresRichVersionFactory extends RichVersionFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(RichVersion.class);

    private PostgresVersionFactory versionFactory;
    private PostgresStructureVersionFactory structureVersionFactory;
    private PostgresTagFactory tagFactory;

    public PostgresRichVersionFactory(PostgresVersionFactory versionFactory,
                                      PostgresStructureVersionFactory structureVersionFactory,
                                      PostgresTagFactory tagFactory) {

        this.versionFactory = versionFactory;
        this.structureVersionFactory = structureVersionFactory;
        this.tagFactory = tagFactory;
    }

    public void insertIntoDatabase(GroundDBConnection connection, String id, Optional<Map<String, Tag>>tags, Optional<String> structureVersionId, Optional<String> reference, Optional<Map<String, String>> parameters) throws GroundException {
        this.versionFactory.insertIntoDatabase(connection, id);

        if(structureVersionId.isPresent()) {
            StructureVersion structureVersion = this.structureVersionFactory.retrieveFromDatabase(structureVersionId.get());
            RichVersionFactory.checkStructureTags(connection, structureVersion, tags);
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

    public RichVersion retrieveFromDatabase(GroundDBConnection connection, String id) throws GroundException {
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
            tagsMap = this.tagFactory.retrieveFromDatabaseById(connection, id);
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

        return RichVersionFactory.construct(id, tags, structureVersionId, reference, parameters);
    }
}
