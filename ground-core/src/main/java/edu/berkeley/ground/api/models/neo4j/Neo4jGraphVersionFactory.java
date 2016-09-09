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

package edu.berkeley.ground.api.models.neo4j;

import edu.berkeley.ground.api.models.GraphVersion;
import edu.berkeley.ground.api.models.GraphVersionFactory;
import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.db.Neo4jClient.Neo4jConnection;
import edu.berkeley.ground.exceptions.EmptyResultException;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.v1.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class Neo4jGraphVersionFactory extends GraphVersionFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jGraphVersionFactory.class);
    private Neo4jClient dbClient;

    private Neo4jGraphFactory graphFactory;
    private Neo4jRichVersionFactory richVersionFactory;

    public Neo4jGraphVersionFactory(Neo4jClient dbClient, Neo4jGraphFactory graphFactory, Neo4jRichVersionFactory richVersionFactory) {
        this.dbClient = dbClient;
        this.graphFactory = graphFactory;
        this.richVersionFactory = richVersionFactory;
    }

    public GraphVersion create(Map<String, Tag> tags,
                               String structureVersionId,
                               String reference,
                               Map<String, String> parameters,
                               String graphId,
                               List<String> edgeVersionIds,
                               List<String> parentIds) throws GroundException {

        Neo4jConnection connection = this.dbClient.getConnection();

        try {
            String id = IdGenerator.generateId(graphId);

            tags = tags.values().stream().collect(Collectors.toMap(Tag::getKey, tag -> new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType())));

            List<DbDataContainer> insertions = new ArrayList<>();
            insertions.add(new DbDataContainer("id", GroundType.STRING, id));
            insertions.add(new DbDataContainer("graph_id", GroundType.STRING, graphId));

            connection.addVertex("GraphVersion", insertions);

            connection.commit(); connection = this.dbClient.getConnection();

            this.richVersionFactory.insertIntoDatabase(connection, id, tags, structureVersionId, reference, parameters);

            connection.commit(); connection = this.dbClient.getConnection();

            for (String edgeVersionId : edgeVersionIds) {
                connection.addEdge("GraphVersionEdge", id, edgeVersionId, new ArrayList<>());
            }

            this.graphFactory.update(connection, graphId, id, parentIds);


            connection.commit();
            LOGGER.info("Created graph version " + id + " in graph " + graphId + ".");

            return GraphVersionFactory.construct(id, tags, structureVersionId, reference, parameters, graphId, edgeVersionIds);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }

    public GraphVersion retrieveFromDatabase(String id) throws GroundException {
        Neo4jConnection connection = this.dbClient.getConnection();

        try {
            RichVersion version = this.richVersionFactory.retrieveFromDatabase(connection, id);

            List<DbDataContainer> predicates = new ArrayList<>();
            predicates.add(new DbDataContainer("id", GroundType.STRING, id));

            Record versionRecord = null;
            try {
                versionRecord = connection.getVertex(predicates);
            } catch (EmptyResultException eer) {
                throw new GroundException("No GraphVersion found with id " + id + ".");
            }

            String graphId = Neo4jClient.getStringFromValue((StringValue) versionRecord.get("v")
                    .asNode().get("graph_id"));

            List<String> returnFields = new ArrayList<>();
            returnFields.add("id");

            List<Record> edgeVersionVertices = connection.getAdjacentVerticesByEdgeLabel("GraphVersionEdge", id, returnFields);
            List<String> edgeVersionIds = new ArrayList<>();

            edgeVersionVertices.stream().forEach(edgeVersionVertex -> edgeVersionIds.add(Neo4jClient.getStringFromValue((StringValue) edgeVersionVertex.get("id"))));

            connection.commit();
            LOGGER.info("Retrieved graph version " + id + " in graph " + graphId + ".");

            return GraphVersionFactory.construct(id, version.getTags(), version.getStructureVersionId(), version.getReference(), version.getParameters(), graphId, edgeVersionIds);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }
}
