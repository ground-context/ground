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
import edu.berkeley.ground.exceptions.EmptyResultException;
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

    public void insertIntoDatabase(GroundDBConnection connectionPointer,
                                   String id,
                                   Map<String, Tag> tags,
                                   String structureVersionId,
                                   String reference,
                                   Map<String, String> parameters) throws GroundException {
        PostgresConnection connection = (PostgresConnection) connectionPointer;

        this.versionFactory.insertIntoDatabase(connection, id);

        if(structureVersionId != null) {
            StructureVersion structureVersion = this.structureVersionFactory.retrieveFromDatabase(structureVersionId);
            RichVersionFactory.checkStructureTags(structureVersion, tags);
        }

        List<DbDataContainer> insertions = new ArrayList<>();
        insertions.add(new DbDataContainer("id", GroundType.STRING, id));
        insertions.add(new DbDataContainer("structure_id", GroundType.STRING, structureVersionId));
        insertions.add(new DbDataContainer("reference", GroundType.STRING, reference));

        connection.insert("RichVersions", insertions);

        for (String key : tags.keySet()) {
            Tag tag = tags.get(key);

            List<DbDataContainer> tagInsertion = new ArrayList<>();
            tagInsertion.add(new DbDataContainer("richversion_id", GroundType.STRING, id));
            tagInsertion.add(new DbDataContainer("key", GroundType.STRING, key));

            if (tag.getValue() != null) {
                tagInsertion.add(new DbDataContainer("value", GroundType.STRING, tag.getValue().toString()));
                tagInsertion.add(new DbDataContainer("type", GroundType.STRING, tag.getValueType().toString()));
            } else {
                tagInsertion.add(new DbDataContainer("value", GroundType.STRING, null));
                tagInsertion.add(new DbDataContainer("type", GroundType.STRING, null));
            }

            connection.insert("Tags", tagInsertion);
        }

        for (String key : parameters.keySet()) {
            List<DbDataContainer> parameterInsertion = new ArrayList<>();

            parameterInsertion.add(new DbDataContainer("richversion_id", GroundType.STRING, id));
            parameterInsertion.add(new DbDataContainer("key", GroundType.STRING, key));
            parameterInsertion.add(new DbDataContainer("value", GroundType.STRING, parameters.get(key)));

            connection.insert("RichVersionExternalParameters", parameterInsertion);
        }
    }

    public RichVersion retrieveFromDatabase(GroundDBConnection connectionPointer, String id) throws GroundException {
        PostgresConnection connection = (PostgresConnection) connectionPointer;

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("id", GroundType.STRING, id));

        QueryResults resultSet;
        try {
            resultSet = connection.equalitySelect("RichVersions", DBClient.SELECT_STAR, predicates);
        } catch (EmptyResultException eer) {
            throw new GroundException("No RichVersion found with id " + id + ".");
        }

        List<DbDataContainer> parameterPredicates = new ArrayList<>();
        parameterPredicates.add(new DbDataContainer("richversion_id", GroundType.STRING, id));
        Map<String, String> parameters = new HashMap<>();

        try {
            QueryResults parameterSet = connection.equalitySelect("RichVersionExternalParameters", DBClient.SELECT_STAR, parameterPredicates);

            do {
                parameters.put(parameterSet.getString(2), parameterSet.getString(3));
            } while (parameterSet.next());
        } catch (EmptyResultException eer) {
            // do nothing; there are no parameters
        }

        Map<String, Tag> tags = tagFactory.retrieveFromDatabaseById(connection, id);

        String reference = resultSet.getString(3);
        String structureVersionId = resultSet.getString(2);

        return RichVersionFactory.construct(id, tags, structureVersionId, reference, parameters);
    }
}
