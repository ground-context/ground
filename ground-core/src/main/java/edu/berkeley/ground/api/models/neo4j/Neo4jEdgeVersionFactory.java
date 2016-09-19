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

import edu.berkeley.ground.api.models.EdgeVersion;
import edu.berkeley.ground.api.models.EdgeVersionFactory;
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

public class Neo4jEdgeVersionFactory extends EdgeVersionFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jEdgeVersionFactory.class);
    private Neo4jClient dbClient;

    private Neo4jEdgeFactory edgeFactory;
    private Neo4jRichVersionFactory richVersionFactory;

    public Neo4jEdgeVersionFactory(Neo4jEdgeFactory edgeFactory, Neo4jRichVersionFactory richVersionFactory, Neo4jClient dbClient) {
        this.dbClient = dbClient;
        this.edgeFactory = edgeFactory;
        this.richVersionFactory = richVersionFactory;
    }

    public EdgeVersion create(Map<String, Tag> tags,
                              String structureVersionId,
                              String reference,
                              Map<String, String> parameters,
                              String edgeId,
                              String fromId,
                              String toId,
                              List<String> parentIds) throws GroundException {

        Neo4jConnection connection = this.dbClient.getConnection();

        try {
            String id = IdGenerator.generateId(edgeId);

            tags = tags.values().stream().collect(Collectors.toMap(Tag::getKey, tag -> new Tag(id, tag.getKey(), tag.getValue(), tag.getValueType())));

            List<DbDataContainer> insertions = new ArrayList<>();
            insertions.add(new DbDataContainer("id", GroundType.STRING, id));
            insertions.add(new DbDataContainer("edge_id", GroundType.STRING, edgeId));
            insertions.add(new DbDataContainer("endpoint_one", GroundType.STRING, fromId));
            insertions.add(new DbDataContainer("endpoint_two", GroundType.STRING, toId));

            connection.addVertex("EdgeVersion", insertions);
            this.richVersionFactory.insertIntoDatabase(connection, id, tags, structureVersionId, reference, parameters);

            connection.addEdge("EdgeVersionConnection", fromId, id, new ArrayList<>());
            connection.addEdge("EdgeVersionConnection", id, toId, new ArrayList<>());

            this.edgeFactory.update(connection, edgeId, id, parentIds);

            connection.commit();
            LOGGER.info("Created edge version " + id + " in edge " + edgeId + ".");

            return EdgeVersionFactory.construct(id, tags, structureVersionId, reference, parameters, edgeId, fromId, toId);
        } catch (GroundException e) {
            connection.abort();
            throw e;
        }
    }

    public EdgeVersion retrieveFromDatabase(String id) throws GroundException {
        Neo4jConnection connection = this.dbClient.getConnection();

        try {
            RichVersion version = this.richVersionFactory.retrieveFromDatabase(connection, id);

            List<DbDataContainer> predicates = new ArrayList<>();
            predicates.add(new DbDataContainer("id", GroundType.STRING, id));

            Record versionRecord = null;
            try {
                versionRecord = connection.getVertex(predicates);
            } catch (EmptyResultException eer) {
                throw new GroundException("No EdgeVersion found with id " + id + ".");
            }

            String edgeId = Neo4jClient.getStringFromValue((StringValue) versionRecord.get("v").asNode()
                    .get("edge_id"));
            String fromId = Neo4jClient.getStringFromValue((StringValue) versionRecord.get("v").asNode()
                    .get("endpoint_one"));
            String toId = Neo4jClient.getStringFromValue((StringValue) versionRecord.get("v").asNode()
                    .get("endpoint_two"));

            connection.commit();
            LOGGER.info("Retrieved edge version " + id + " in edge " + edgeId + ".");

            return EdgeVersionFactory.construct(id, version.getTags(), version.getStructureVersionId(), version.getReference(), version.getParameters(), edgeId, fromId, toId);
        } catch (GroundException e) {
            connection.abort();

            throw e;
        }
    }
}
