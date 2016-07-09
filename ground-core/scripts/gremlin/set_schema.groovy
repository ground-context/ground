g = TitanFactory.open('/Users/vikram/Code/titan/conf/titan-cassandra.properties')
management = g.openManagement()

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
