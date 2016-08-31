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

package edu.berkeley.ground.api.versions.gremlin;

import edu.berkeley.ground.api.versions.*;
import edu.berkeley.ground.db.DBClient.GroundDBConnection;
import edu.berkeley.ground.db.DbDataContainer;
import edu.berkeley.ground.db.GremlinClient.GremlinConnection;
import edu.berkeley.ground.exceptions.GroundException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.List;

public class GremlinVersionHistoryDAGFactory extends VersionHistoryDAGFactory {
    private GremlinVersionSuccessorFactory versionSuccessorFactory;

    public GremlinVersionHistoryDAGFactory(GremlinVersionSuccessorFactory versionSuccessorFactory) {
        this.versionSuccessorFactory = versionSuccessorFactory;
    }

    public <T extends Version> VersionHistoryDAG<T> create(String itemId) throws GroundException {
        return construct(itemId);
    }

    public <T extends Version> VersionHistoryDAG<T> retrieveFromDatabase(GroundDBConnection connectionPointer, String itemId) throws GroundException {
        GremlinConnection connection = (GremlinConnection) connectionPointer;

        List<DbDataContainer> predicates = new ArrayList<>();
        predicates.add(new DbDataContainer("id", GroundType.STRING, itemId));

        Vertex itemVertex = connection.getVertex(predicates);
        if (itemVertex == null) {
            throw new GroundException("No results found for query");
        }

        List<Edge> gremlinEdges = connection.getDescendantEdgesWithLabel(itemVertex, "VersionSuccessor");

        List<VersionSuccessor<T>> edges = new ArrayList<>();

        for (Edge gremlinEdge : gremlinEdges) {
            edges.add(this.versionSuccessorFactory.retrieveFromDatabase(connection, (String) gremlinEdge.property("successor_id").value()));
        }

        return VersionHistoryDAGFactory.construct(itemId, edges);
    }

    public void addEdge(GroundDBConnection connectionPointer, VersionHistoryDAG dag, String parentId, String childId, String itemId) throws GroundException {
        VersionSuccessor successor = this.versionSuccessorFactory.create(connectionPointer, parentId, childId);

        dag.addEdge(parentId, childId, successor.getId());
    }
}
