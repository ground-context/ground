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

import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.StructureVersionFactory;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.Neo4jClient;
import edu.berkeley.ground.db.Neo4jClient.Neo4jConnection;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.util.IdGenerator;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Neo4jStructureVersionFactory extends StructureVersionFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(Neo4jStructureVersionFactory.class);
    private Neo4jClient dbClient;
    private Neo4jStructureFactory structureFactory;

    public Neo4jStructureVersionFactory(Neo4jClient dbClient, Neo4jStructureFactory structureFactory) {
        this.dbClient = dbClient;
        this.structureFactory = structureFactory;
    }

    public StructureVersion create(String structureId, Map<String, GroundType> attributes, List<String> parentIds) throws GroundException {
        Neo4jConnection connection = this.dbClient.getConnection();

        String id = IdGenerator.generateId(structureId);

        List<DbDataContainer> insertions = new ArrayList<>();
        insertions.add(new DbDataContainer("id", GroundType.STRING, id));
        insertions.add(new DbDataContainer("structure_id", GroundType.STRING, structureId));

        connection.addVertex("StructureVersion", insertions);

        for (String key : attributes.keySet()) {
            List<DbDataContainer> itemInsertions = new ArrayList<>();
            itemInsertions.add(new DbDataContainer("svid", GroundType.STRING, id));
            itemInsertions.add(new DbDataContainer("skey", GroundType.STRING, key));
            itemInsertions.add(new DbDataContainer("stype", GroundType.STRING, attributes.get(key).toString()));

            connection.addVertexAndEdge("StructureVersionItem", itemInsertions, "StructureVersionItemConnection", id, new ArrayList<>());
        }

        this.structureFactory.update(connection, structureId, id, parentIds);

        connection.commit();
        LOGGER.info("Created structure version " + id + " in structure " + structureId + ".");

        return StructureVersionFactory.construct(id, structureId, attributes);
    }

    public StructureVersion retrieveFromDatabase(String id) throws GroundException {
        Neo4jConnection connection = this.dbClient.getConnection();

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("id", GroundType.STRING, id));

        String structureId = connection.getVertex(predicates).get("structure_id").toString();
        List<String> returnFields = new ArrayList<>();
        returnFields.add("svid");
        returnFields.add("skey");
        returnFields.add("stype");

        List<Record> edges = connection.getAdjacentVerticesByEdgeLabel("StructureVersionItemConnection", id, returnFields);
        Map<String, GroundType> attributes = new HashMap<>();


        for (Record record : edges) {
            attributes.put(Neo4jClient.getStringFromValue((StringValue) record.get("skey")), GroundType.fromString( Neo4jClient.getStringFromValue((StringValue) record.get("stype"))));
        }

        return StructureVersionFactory.construct(id, structureId, attributes);
    }

}
