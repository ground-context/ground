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
import edu.berkeley.ground.db.PostgresClient.PostgresConnection;
import edu.berkeley.ground.db.QueryResults;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.*;

public class PostgresRichVersionFactory extends RichVersionFactory {
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

    public void insertIntoDatabase(GroundDBConnection connectionPointer, String id, Optional<Map<String, Tag>>tags, Optional<String> structureVersionId, Optional<String> reference, Optional<Map<String, String>> parameters) throws GroundException {
        PostgresConnection connection = (PostgresConnection) connectionPointer;

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

    public RichVersion retrieveFromDatabase(GroundDBConnection connectionPointer, String id) throws GroundException {
        PostgresConnection connection = (PostgresConnection) connectionPointer;

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("id", Type.STRING, id));
        QueryResults resultSet = connection.equalitySelect("RichVersions", DBClient.SELECT_STAR, predicates);

        List<DbDataContainer> parameterPredicates = new ArrayList<>();
        parameterPredicates.add(new DbDataContainer("richversion_id", Type.STRING, id));
        Map<String, String> parametersMap = new HashMap<>();
        Optional<Map<String, String>> parameters;
        try {
            QueryResults parameterSet = connection.equalitySelect("RichVersionExternalParameters", DBClient.SELECT_STAR, parameterPredicates);

            do {
                parametersMap.put(parameterSet.getString(2), parameterSet.getString(3));
            } while (parameterSet.next());

            parameters = Optional.of(parametersMap);
        } catch (GroundException e) {
            if (e.getMessage().contains("No results found for query")) {
                parameters = Optional.empty();
            } else {
                throw e;
            }
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

        Optional<String> reference = Optional.ofNullable(resultSet.getString(3));
        Optional<String> structureVersionId = Optional.ofNullable(resultSet.getString(2));

        return RichVersionFactory.construct(id, tags, structureVersionId, reference, parameters);
    }
}
