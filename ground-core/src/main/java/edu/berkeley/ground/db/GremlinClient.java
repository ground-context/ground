package edu.berkeley.ground.db;

import edu.berkeley.ground.exceptions.GroundDBException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class GremlinClient implements DBClient {
    private Graph graph;

    public GremlinClient() {
        this.graph = GraphFactory.open("conf/titan-cassandra.properties");
    }

    public GremlinConnection getConnection() throws GroundDBException {
        return new GremlinConnection(this.graph);
    }

    public class GremlinConnection extends GroundDBConnection {
        private Graph graph;

        protected GremlinConnection(Graph graph) {
            this.graph = graph;
        }

        public Vertex addVertex(String label, List<DbDataContainer> attributes) {
            Object[] attributesArray = new Object[(attributes.size() + 1) * 2];
            for(int i = 1; i < attributes.size() + 1; i++) {
                attributesArray[2*i] = attributes.get(i - 1).getField();
                attributesArray[2*i + 1] = attributes.get(i - 1).getValue();
            }

            attributesArray[0] = T.label;
            attributesArray[1] = label;

            return this.graph.addVertex(attributesArray);
        }

        public void addEdge(String label, Vertex source, Vertex destination, List<DbDataContainer> attributes) {
            Object[] attributesArray = new Object[attributes.size() * 2];
            for(int i = 0; i < attributes.size(); i++) {
                attributesArray[2*i] = attributes.get(i).getField();
                attributesArray[2*i + 1] = attributes.get(i).getValue();
            }

            source.addEdge(label, destination, attributesArray);
        }

        public Vertex getVertex(List<DbDataContainer> predicates) {
            GraphTraversal traversal = this.graph.traversal().V();

            for (DbDataContainer predicate : predicates) {
                traversal = traversal.has(predicate.getField(), predicate.getValue());
            }

            if (traversal.hasNext()) {
                return (Vertex) traversal.next();
            }

            return null;
        }

        public Edge getEdge(List<DbDataContainer> predicates) {
            GraphTraversal traversal = this.graph.traversal().E();

            for (DbDataContainer predicate : predicates) {
                traversal = traversal.has(predicate.getField(), predicate.getValue());
            }

            if (traversal.hasNext()) {
                return (Edge) traversal.next();
            }

            return null;
        }

        public List<Edge> getDescendantEdgesWithLabel(Vertex vertex, String label) {
            List<Edge> result = new ArrayList<>();
            LinkedList<Vertex> queue = new LinkedList<>();
            queue.add(vertex);

            while(!queue.isEmpty()) {
                Vertex current = queue.pop();
                Iterator<Edge> currentEdges = current.edges(Direction.OUT, label);

                currentEdges.forEachRemaining(edge -> {
                    queue.add(edge.inVertex());
                    result.add(edge);
                });
            }

            return result;
        }

        public List<Vertex> getAdjacentVerticesByEdgeLabel(Vertex vertex, String edgeLabel) {
            List<Vertex> result = new ArrayList<>();
            vertex.vertices(Direction.BOTH, edgeLabel).forEachRemaining(result::add);

            return result;
        }

        public List<String> transitiveClosure(String nodeVersionId) {
            GraphTraversal traversal = this.graph.traversal().V().has("id", nodeVersionId);

            traversal = traversal.repeat(__.outE("EdgeVersionConnection").inV().outE("EdgeVersionConnection").inV());
            traversal = traversal.emit();
            traversal = traversal.until(__.outE("EdgeVersionConnection").count().is(0)).values("id");

            List<String> result = new ArrayList<>();

            traversal.forEachRemaining(object -> {
                result.add(((String) object));
            });

            return result;
        }

        public void commit() throws GroundDBException {
            this.graph.tx().commit();
        }

        public void abort() throws GroundDBException {
            this.graph.tx().rollback();
        }
    }
}
