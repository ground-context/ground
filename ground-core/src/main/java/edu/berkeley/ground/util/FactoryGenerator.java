package edu.berkeley.ground.util;

import edu.berkeley.ground.api.models.EdgeFactory;
import edu.berkeley.ground.api.models.EdgeVersionFactory;
import edu.berkeley.ground.api.models.GraphFactory;
import edu.berkeley.ground.api.models.GraphVersionFactory;
import edu.berkeley.ground.api.models.NodeFactory;
import edu.berkeley.ground.api.models.NodeVersionFactory;
import edu.berkeley.ground.api.models.StructureFactory;
import edu.berkeley.ground.api.models.StructureVersionFactory;
import edu.berkeley.ground.api.usage.LineageEdgeFactory;
import edu.berkeley.ground.api.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.api.usage.LineageGraphFactory;
import edu.berkeley.ground.api.usage.LineageGraphVersionFactory;

public interface FactoryGenerator {
  EdgeFactory getEdgeFactory();

  EdgeVersionFactory getEdgeVersionFactory();

  GraphFactory getGraphFactory();

  GraphVersionFactory getGraphVersionFactory();

  NodeFactory getNodeFactory();

  NodeVersionFactory getNodeVersionFactory();

  LineageEdgeFactory getLineageEdgeFactory();

  LineageEdgeVersionFactory getLineageEdgeVersionFactory();

  StructureFactory getStructureFactory();

  StructureVersionFactory getStructureVersionFactory();

  LineageGraphFactory getLineageGraphFactory();

  LineageGraphVersionFactory getLineageGraphVersionFactory();
}
