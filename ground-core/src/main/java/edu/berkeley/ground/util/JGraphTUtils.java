package edu.berkeley.ground.util;

import edu.berkeley.ground.exceptions.GroundDBException;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.DepthFirstIterator;

import java.util.ArrayList;
import java.util.List;

public class JGraphTUtils {
    public static DirectedGraph<String, DefaultEdge> createGraph() throws GroundDBException {
        return new DefaultDirectedGraph<>(DefaultEdge.class);
    }

    public static void addVertex(DirectedGraph<String, DefaultEdge> graph, String id) {
        graph.addVertex(id);
    }

    public static void addEdge(DirectedGraph<String, DefaultEdge> graph, String from, String to) {
        graph.addEdge(from, to);
    }

    public static List<String> iterate(DirectedGraph<String, DefaultEdge> graph, String start) throws GroundDBException {
        DepthFirstIterator<String, DefaultEdge> iterator = new DepthFirstIterator<>(graph, start);

        List<String> result = new ArrayList<>();

        while (iterator.hasNext()) {
            result.add(iterator.next());
        }

        return result;
    }
}
