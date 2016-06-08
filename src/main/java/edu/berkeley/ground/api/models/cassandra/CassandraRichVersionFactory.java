package edu.berkeley.ground.api.models.cassandra;

import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.RichVersionFactory;
import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.Type;
import edu.berkeley.ground.api.versions.cassandra.CassandraVersionFactory;
import edu.berkeley.ground.db.CassandraClient.CassandraConnection;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.QueryResults;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.ElasticSearchClient;

import java.util.*;

public class CassandraRichVersionFactory extends RichVersionFactory {
    private CassandraVersionFactory versionFactory;
    private CassandraStructureVersionFactory structureVersionFactory;
    private CassandraTagFactory tagFactory;
    private ElasticSearchClient elasticSearchClient;

    public CassandraRichVersionFactory(CassandraVersionFactory versionFactory,
                                       CassandraStructureVersionFactory structureVersionFactory,
                                       CassandraTagFactory tagFactory,
                                       ElasticSearchClient elasticSearchClient) {

        this.versionFactory = versionFactory;
        this.structureVersionFactory = structureVersionFactory;
        this.tagFactory = tagFactory;
        this.elasticSearchClient = elasticSearchClient;
    }

    public void insertIntoDatabase(GroundDBConnection connectionPointer, String id, Optional<Map<String, Tag>>tags, Optional<String> structureVersionId, Optional<String> reference, Optional<Map<String, String>> parameters) throws GroundException {
        CassandraConnection connection = (CassandraConnection) connectionPointer;
        this.versionFactory.insertIntoDatabase(connection, id);

        if(structureVersionId.isPresent()) {
            StructureVersion structureVersion = this.structureVersionFactory.retrieveFromDatabase(structureVersionId.get());

            RichVersionFactory.checkStructureTags(structureVersion, tags);
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
                tagInsertion.add(new DbDataContainer("value", Type.STRING, tag.getValue().map(Object::toString).orElse(null)));
                tagInsertion.add(new DbDataContainer("type", Type.STRING, tag.getValueType().map(Type::toString).orElse(null)));

                connection.insert("Tags", tagInsertion);
            }

            this.elasticSearchClient.indexTags(id, tags.get());
        }
    }

    public RichVersion retrieveFromDatabase(GroundDBConnection connectionPointer, String id) throws GroundException {
        CassandraConnection connection = (CassandraConnection) connectionPointer;

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
                parametersMap.put(parameterSet.getString(0), parameterSet.getString(1));
            } while (parameterSet.next());

            parameters = Optional.of(parametersMap);
        } catch (GroundException e) {
            if (e.getMessage().contains("No results found for query")) {
                parameters = Optional.empty();
            } else {
                throw e;
            }
        }

        Optional<Map<String, Tag>> tags = tagFactory.retrieveFromDatabaseById(connection, id);

        Optional<String> reference = Optional.ofNullable(resultSet.getString(2));
        Optional<String> structureVersionId = Optional.ofNullable(resultSet.getString(1));

        return RichVersionFactory.construct(id, tags, structureVersionId, reference, parameters);
    }
}
