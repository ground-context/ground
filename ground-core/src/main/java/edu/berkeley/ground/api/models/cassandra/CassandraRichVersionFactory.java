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

package edu.berkeley.ground.api.models.cassandra;

import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.RichVersionFactory;
import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.api.versions.cassandra.CassandraVersionFactory;
import edu.berkeley.ground.db.CassandraClient.CassandraConnection;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.QueryResults;
import edu.berkeley.ground.exceptions.GroundException;

import java.util.*;

public class CassandraRichVersionFactory extends RichVersionFactory {
    private CassandraVersionFactory versionFactory;
    private CassandraStructureVersionFactory structureVersionFactory;
    private CassandraTagFactory tagFactory;

    public CassandraRichVersionFactory(CassandraVersionFactory versionFactory,
                                       CassandraStructureVersionFactory structureVersionFactory,
                                       CassandraTagFactory tagFactory) {

        this.versionFactory = versionFactory;
        this.structureVersionFactory = structureVersionFactory;
        this.tagFactory = tagFactory;
    }

    public void insertIntoDatabase(GroundDBConnection connectionPointer,
                                   String id,
                                   Map<String, Tag>tags,
                                   String structureVersionId,
                                   String reference,
                                   Map<String, String> parameters) throws GroundException {
        CassandraConnection connection = (CassandraConnection) connectionPointer;
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
            tagInsertion.add(new DbDataContainer("value", GroundType.STRING, tag.getValue()));
            tagInsertion.add(new DbDataContainer("type", GroundType.STRING, tag.getValueType()));

            connection.insert("Tags", tagInsertion);
        }
    }

    public RichVersion retrieveFromDatabase(GroundDBConnection connectionPointer, String id) throws GroundException {
        CassandraConnection connection = (CassandraConnection) connectionPointer;

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("id", GroundType.STRING, id));
        QueryResults resultSet = connection.equalitySelect("RichVersions", DBClient.SELECT_STAR, predicates);

        List<DbDataContainer> parameterPredicates = new ArrayList<>();
        parameterPredicates.add(new DbDataContainer("richversion_id", GroundType.STRING, id));
        Map<String, String> parameters = new HashMap<>();
        try {
            QueryResults parameterSet = connection.equalitySelect("RichVersionExternalParameters", DBClient.SELECT_STAR, parameterPredicates);

            do {
                parameters.put(parameterSet.getString(0), parameterSet.getString(1));
            } while (parameterSet.next());
        } catch (GroundException e) {
            if (!e.getMessage().contains("No results found for query")) {
                throw e;
            }
        }

        Map<String, Tag> tags = tagFactory.retrieveFromDatabaseById(connection, id);

        String reference = resultSet.getString(2);
        String structureVersionId = resultSet.getString(1);

        return RichVersionFactory.construct(id, tags, structureVersionId, reference, parameters);
    }
}
