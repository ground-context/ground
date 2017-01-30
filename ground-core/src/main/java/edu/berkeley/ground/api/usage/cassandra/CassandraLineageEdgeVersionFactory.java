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

package edu.berkeley.ground.api.usage.cassandra;

import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.models.cassandra.CassandraRichVersionFactory;
import edu.berkeley.ground.api.usage.LineageEdgeVersion;
import edu.berkeley.ground.api.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.db.CassandraClient;
import edu.berkeley.ground.db.CassandraClient.CassandraConnection;
import edu.berkeley.ground.db.DBClient;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.QueryResults;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CassandraLineageEdgeVersionFactory extends LineageEdgeVersionFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraLineageEdgeVersionFactory.class);
    private CassandraClient dbClient;

    private CassandraLineageEdgeFactory lineageEdgeFactory;
    private CassandraRichVersionFactory richVersionFactory;

    public CassandraLineageEdgeVersionFactory(CassandraLineageEdgeFactory lineageEdgeFactory, CassandraRichVersionFactory richVersionFactory, CassandraClient dbClient) {
        this.dbClient = dbClient;
        this.lineageEdgeFactory = lineageEdgeFactory;
        this.richVersionFactory = richVersionFactory;
    }


    public LineageEdgeVersion create(Map<String, Tag> tags,
                                     String structureVersionId,
                                     String reference,
                                     Map<String, String> referenceParameters,
                                     String fromId,
                                     String toId,
                                     String lineageEdgeId,
                                     List<String> parentIds) throws GroundException {

        CassandraConnection connection = this.dbClient.getConnection();

        try {
            String id = IdGenerator.generateId(lineageEdgeId);

            tags.values().stream().collect(Collectors.toMap(Tag::getKey, tag -> new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType())));

            this.richVersionFactory.insertIntoDatabase(connection, id, tags, structureVersionId, reference, referenceParameters);

            List<DbDataContainer> insertions = new ArrayList<>();
            insertions.add(new DbDataContainer("id", GroundType.STRING, id));
            insertions.add(new DbDataContainer("lineageedge_id", GroundType.STRING, lineageEdgeId));
            insertions.add(new DbDataContainer("endpoint_one", GroundType.STRING, fromId));
            insertions.add(new DbDataContainer("endpoint_two", GroundType.STRING, toId));

            connection.insert("LineageEdgeVersions", insertions);

            this.lineageEdgeFactory.update(connection, lineageEdgeId, id, parentIds);

            connection.commit();
            LOGGER.info("Created lineage edge version " + id + " in lineage edge " + lineageEdgeId + ".");

            return LineageEdgeVersionFactory.construct(id, tags, structureVersionId, reference, referenceParameters, fromId, toId, lineageEdgeId);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }

    public LineageEdgeVersion retrieveFromDatabase(String id) throws GroundException {
        CassandraConnection connection = this.dbClient.getConnection();

        try {
            RichVersion version = this.richVersionFactory.retrieveFromDatabase(connection, id);

            List<DbDataContainer> predicates = new ArrayList<>();
            predicates.add(new DbDataContainer("id", GroundType.STRING, id));

            QueryResults resultSet;
            try {
                resultSet = connection.equalitySelect("LineageEdgeVersions", DBClient.SELECT_STAR, predicates);
            } catch (EmptyResultException eer) {
                throw new GroundException("No LineageEdgeVersion found with id " + id + ".");
            }

            if (!resultSet.next()) {
                throw new GroundException("No LineageEdgeVersion found with id " + id + ".");
            }

            String lineageEdgeId = resultSet.getString("lineageedge_id");
            String fromId = resultSet.getString("endpoint_one");
            String toId = resultSet.getString("endpoint_two");

            connection.commit();
            LOGGER.info("Retrieved lineage edge version " + id + " in lineage edge " + lineageEdgeId + ".");

            return LineageEdgeVersionFactory.construct(id, version.getTags(), version.getStructureVersionId(), version.getReference(), version.getParameters(), fromId, toId, lineageEdgeId);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }
}
