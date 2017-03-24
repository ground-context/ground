package edu.berkeley.ground.util;

import edu.berkeley.ground.dao.models.EdgeFactory;
import edu.berkeley.ground.dao.models.EdgeVersionFactory;
import edu.berkeley.ground.dao.models.GraphFactory;
import edu.berkeley.ground.dao.models.GraphVersionFactory;
import edu.berkeley.ground.dao.models.NodeFactory;
import edu.berkeley.ground.dao.models.NodeVersionFactory;
import edu.berkeley.ground.dao.models.StructureFactory;
import edu.berkeley.ground.dao.models.StructureVersionFactory;
import edu.berkeley.ground.dao.usage.LineageEdgeFactory;
import edu.berkeley.ground.dao.usage.LineageEdgeVersionFactory;
import edu.berkeley.ground.dao.usage.LineageGraphFactory;
import edu.berkeley.ground.dao.usage.LineageGraphVersionFactory;

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
