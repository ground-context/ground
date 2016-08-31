/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.api.models.postgres;

import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.RichVersionFactory;
import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
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
            RichVersionFactory.checkStructureTags(structureVersion, tags);
        }

        List<DbDataContainer> insertions = new ArrayList<>();
        insertions.add(new DbDataContainer("id", GroundType.STRING, id));
        insertions.add(new DbDataContainer("structure_id", GroundType.STRING, structureVersionId.orElse(null)));
        insertions.add(new DbDataContainer("reference", GroundType.STRING, reference.orElse(null)));

        connection.insert("RichVersions", insertions);

        if (tags.isPresent()) {
            for (String key : tags.get().keySet()) {
                Tag tag = tags.get().get(key);

                List<DbDataContainer> tagInsertion = new ArrayList<>();
                tagInsertion.add(new DbDataContainer("richversion_id", GroundType.STRING, id));
                tagInsertion.add(new DbDataContainer("key", GroundType.STRING, key));
                tagInsertion.add(new DbDataContainer("value", GroundType.STRING, tag.getValue().map(Object::toString).orElse(null)));
                tagInsertion.add(new DbDataContainer("type", GroundType.STRING, tag.getValueType().map(Object::toString).orElse(null)));

                connection.insert("Tags", tagInsertion);
            }
        }
    }

    public RichVersion retrieveFromDatabase(GroundDBConnection connectionPointer, String id) throws GroundException {
        PostgresConnection connection = (PostgresConnection) connectionPointer;

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("id", GroundType.STRING, id));
        QueryResults resultSet = connection.equalitySelect("RichVersions", DBClient.SELECT_STAR, predicates);

        List<DbDataContainer> parameterPredicates = new ArrayList<>();
        parameterPredicates.add(new DbDataContainer("richversion_id", GroundType.STRING, id));
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

        Optional<Map<String, Tag>> tags = tagFactory.retrieveFromDatabaseById(connection, id);

        Optional<String> reference = Optional.ofNullable(resultSet.getString(3));
        Optional<String> structureVersionId = Optional.ofNullable(resultSet.getString(2));

        return RichVersionFactory.construct(id, tags, structureVersionId, reference, parameters);
    }
}
