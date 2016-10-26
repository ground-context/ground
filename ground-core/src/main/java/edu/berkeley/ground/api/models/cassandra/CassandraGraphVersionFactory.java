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

import edu.berkeley.ground.api.models.GraphVersion;
import edu.berkeley.ground.api.models.GraphVersionFactory;
import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.Tag;
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
import java.util.Optional;
import java.util.stream.Collectors;

public class CassandraGraphVersionFactory extends GraphVersionFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraGraphVersionFactory.class);
    private CassandraClient dbClient;

    private CassandraGraphFactory graphFactory;
    private CassandraRichVersionFactory richVersionFactory;

    public CassandraGraphVersionFactory(CassandraGraphFactory graphFactory, CassandraRichVersionFactory richVersionFactory, CassandraClient dbClient) {
        this.dbClient = dbClient;
        this.graphFactory = graphFactory;
        this.richVersionFactory = richVersionFactory;
    }

    public GraphVersion create(Map<String, Tag> tags,
                               String structureVersionId,
                               String reference,
                               Map<String, String> referenceParameters,
                               String graphId,
                               List<String> edgeVersionIds,
                               List<String> parentIds) throws GroundException {

        CassandraConnection connection = this.dbClient.getConnection();

        try {
            String id = IdGenerator.generateId(graphId);

            tags = tags.values().stream().collect(Collectors.toMap(Tag::getKey, tag -> new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType())));

            this.richVersionFactory.insertIntoDatabase(connection, id, tags, structureVersionId, reference, referenceParameters);

            List<DbDataContainer> insertions = new ArrayList<>();
            insertions.add(new DbDataContainer("id", GroundType.STRING, id));
            insertions.add(new DbDataContainer("graph_id", GroundType.STRING, graphId));

            connection.insert("GraphVersions", insertions);

            for (String edgeVersionId : edgeVersionIds) {
                List<DbDataContainer> edgeInsertion = new ArrayList<>();
                edgeInsertion.add(new DbDataContainer("gvid", GroundType.STRING, id));
                edgeInsertion.add(new DbDataContainer("evid", GroundType.STRING, edgeVersionId));

                connection.insert("GraphVersionEdges", edgeInsertion);
            }

            this.graphFactory.update(connection, graphId, id, parentIds);

            connection.commit();
            LOGGER.info("Created graph version " + id + " in graph " + graphId + ".");

            return GraphVersionFactory.construct(id, tags, structureVersionId, reference, referenceParameters, graphId, edgeVersionIds);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }

    public GraphVersion retrieveFromDatabase(String id) throws GroundException {
        CassandraConnection connection = this.dbClient.getConnection();

        try {
            RichVersion version = this.richVersionFactory.retrieveFromDatabase(connection, id);

            List<DbDataContainer> predicates = new ArrayList<>();
            predicates.add(new DbDataContainer("id", GroundType.STRING, id));

            List<DbDataContainer> edgePredicate = new ArrayList<>();
            edgePredicate.add(new DbDataContainer("gvid", GroundType.STRING, id));

            QueryResults resultSet;
            try {
                resultSet = connection.equalitySelect("GraphVersions", DBClient.SELECT_STAR, predicates);
            } catch (EmptyResultException eer) {
                throw new GroundException("No GraphVersion found with id " + id + ".");
            }

            if (!resultSet.next()) {
                throw new GroundException("No GraphVersion found with id " + id + ".");
            }

            String graphId = resultSet.getString(1);

            List<String> edgeVersionIds = new ArrayList<>();
            try {
                QueryResults edgeSet = connection.equalitySelect("GraphVersionEdges", DBClient.SELECT_STAR, edgePredicate);
                edgeVersionIds = edgeSet.getStringList(1);
            } catch (EmptyResultException eer) {
                // do nothing; this means that the graph is empty
            }


            connection.commit();
            LOGGER.info("Retrieved graph version " + id + " in graph " + graphId + ".");

            return GraphVersionFactory.construct(id, version.getTags(), version.getStructureVersionId(), version.getReference(), version.getParameters(), graphId, edgeVersionIds);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }
}
