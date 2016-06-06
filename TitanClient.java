package edu.berkeley.ground.db;

import com.google.common.annotations.VisibleForTesting;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import edu.berkeley.ground.exceptions.GroundDBException;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class TitanClient implements DBClient {
    private static final String DIRECTORY = "./titandb/";

    private TitanGraph graph;

    public TitanClient(boolean createSchema) {
        TitanFactory.Builder config = TitanFactory.build();

        config.set("storage.backend", "cassandra");
        config.set("storage.hostname", "localhost");
        config.set("storage.port", 9160);
        config.set("storage.directory", DIRECTORY);

        this.graph = config.open();

        if(createSchema) {
            this.createSchema();
        }
    }

    public TitanConnection getConnection() throws GroundDBException {
        return new TitanConnection(graph.newTransaction());
    }

    public class TitanConnection extends GroundDBConnection {
        TitanTransaction transaction;

        protected TitanConnection(TitanTransaction transaction) {
            this.transaction = transaction;
        }

        public TitanVertex addVertex(String label, List<DbDataContainer> attributes) {
            Object[] attributesArray = new Object[(attributes.size() + 1) * 2];
            for(int i = 1; i < attributes.size() + 1; i++) {
                attributesArray[2*i] = attributes.get(i - 1).getField();
                attributesArray[2*i + 1] = attributes.get(i - 1).getValue();
            }

            attributesArray[0] = T.label;
            attributesArray[1] = label;

            return this.transaction.addVertex(attributesArray);
        }

        public void addEdge(String label, TitanVertex source, TitanVertex destination, List<DbDataContainer> attributes) {
            Object[] attributesArray = new Object[attributes.size() * 2];
            for(int i = 0; i < attributes.size(); i++) {
                attributesArray[2*i] = attributes.get(i).getField();
                attributesArray[2*i + 1] = attributes.get(i).getValue();
            }


            source.addEdge(label, destination, attributesArray);
        }

        public TitanVertex getVertex(List<DbDataContainer> predicates) {
            TitanGraphQuery query = this.transaction.query();
            for(DbDataContainer predicate : predicates) {
                query = query.has(predicate.getField(), predicate.getValue());
            }

            return (TitanVertex) query.vertices().iterator().next();
        }

        public TitanEdge getEdge(List<DbDataContainer> predicates) {
            TitanGraphQuery query = this.transaction.query();
            for(DbDataContainer predicate : predicates) {
                query = query.has(predicate.getField(), predicate.getValue());
            }

            return (TitanEdge) query.edges().iterator().next();
        }

        public List<TitanEdge> getDescendantEdgesWithLabel(TitanVertex vertex, String label) {
            List<TitanEdge> result = new ArrayList<>();
            LinkedList<TitanVertex> queue = new LinkedList<>();
            queue.add(vertex);

            TitanVertexQuery query;
            while ((query = queue.pop().query().has("label", label)).count() != 0) {
                Iterator<TitanEdge> edgeIterator = query.edges().iterator();
                edgeIterator.forEachRemaining(edge -> {
                    queue.add(edge.outVertex());
                    result.add(edge);
                });
            }

            return result;
        }

        public List<TitanVertex> getAdjacentVerticesByEdgeLabel(TitanVertex vertex, String edgeLabel) {
            List<TitanVertex> result = new ArrayList<>();

            TitanVertexQuery query = vertex.query().has("label", edgeLabel);
            Iterator<TitanEdge> edgeIterator = query.edges().iterator();

            edgeIterator.forEachRemaining(edge -> result.add(edge.outVertex()));

            return result;
        }

        public void commit() throws GroundDBException {
            this.transaction.commit();
        }

        public void abort() throws GroundDBException {
            this.transaction.rollback();
        }

        @Override
        public void beginTransaction() throws GroundDBException {
          // TODO Auto-generated method stub
        }
    }

    @VisibleForTesting
    public void createSchema() {
        TitanManagement management = this.graph.openManagement();

        management.makeVertexLabel("GroundEdge").make();
        management.makeVertexLabel("EdgeVersion").make();
        management.makeVertexLabel("Graph").make();
        management.makeVertexLabel("GraphVersion").make();
        management.makeVertexLabel("LineageEdge").make();
        management.makeVertexLabel("LineageEdgeVersion").make();
        management.makeVertexLabel("Node").make();
        management.makeVertexLabel("NodeVersion").make();
        management.makeVertexLabel("Structure").make();
        management.makeVertexLabel("StructureVersion").make();
        management.makeVertexLabel("StructureVersionItem").make();
        management.makeVertexLabel("Tag").make();
        management.makeVertexLabel("RichVersionExternalParameter").make();

        management.makeEdgeLabel("VersionSuccessor").multiplicity(Multiplicity.SIMPLE).make();
        management.makeEdgeLabel("StructureVersionItemConnection").multiplicity(Multiplicity.ONE2MANY).make();
        management.makeEdgeLabel("TagConnection").multiplicity(Multiplicity.ONE2MANY).make();
        management.makeEdgeLabel("RichVersionExternalParameterConnection").multiplicity(Multiplicity.ONE2MANY).make();
        management.makeEdgeLabel("EdgeVersionConnection").multiplicity(Multiplicity.SIMPLE).make();
        management.makeEdgeLabel("GraphVersionEdge").multiplicity(Multiplicity.ONE2MANY).make();
        management.makeEdgeLabel("LineageEdgeVersionConnection").multiplicity(Multiplicity.SIMPLE).make();

        management.commit();
    }
}
