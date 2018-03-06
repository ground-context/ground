import unittest
import ground


class GroundTest(unittest.TestCase):
    def test_node_attr(self):
        node = ground.Node('testSourceKey', 'testName', {'testKey': 'testValue'})
        self.assertIsNotNone(node.sourceKey, "node attribute 'sourceKey' is None")
        self.assertEqual(node.sourceKey, 'testSourceKey', "node attribute 'sourceKey', "
                                                          "Expected: testSourceKey, Actual: " + str(node.sourceKey))
        self.assertIsNotNone(node.name, "node attribute 'name' is None")
        self.assertEqual(node.name, 'testName', "node attribute 'name', Expected: testName, "
                                                "Actual: " + str(node.name))
        self.assertIsNotNone(node.tags, "node attribute 'tags' is None")
        self.assertEqual(node.tags, {'testKey': 'testValue'},
                         "node attribute 'sourceKey', Expected: "
                         "" + str({'testKey': 'testValue'}) + ", Actual: " + str(node.tags))
        node_json = node.to_json()
        self.assertEqual(node_json, '{"nodeId": null, "class": "Node", "sourceKey": "testSourceKey", '
                                    '"name": "testName", "tags": {"testKey": "testValue"}}')

    def test_edge_attr(self):
        edge = ground.Edge('testSourceKey', 0, 1, 'testName', {'testKey': 'testValue'})
        self.assertIsNotNone(edge.sourceKey, "edge attribute 'sourceKey' is None")
        self.assertEqual(edge.sourceKey, 'testSourceKey', "edge attribute 'sourceKey', "
                                                          "Expected: testSourceKey, Actual: " + str(edge.sourceKey))
        self.assertIsNotNone(edge.fromNodeId, "edge attribute 'fromNodeId' is None")
        self.assertEqual(edge.fromNodeId, 0, "edge attribute 'fromNodeId', Expected: 0, "
                                             "Actual: " + str(edge.fromNodeId))
        self.assertIsNotNone(edge.toNodeId, "edge attribute 'toNodeId' is None")
        self.assertEqual(edge.toNodeId, 1, "edge attribute 'toNodeId', Expected: 1, "
                                           "Actual: " + str(edge.toNodeId))
        self.assertIsNotNone(edge.name, "edge attribute 'name' is None")
        self.assertEqual(edge.name, 'testName', "edge attribute 'name', Expected: testName, "
                                                "Actual: " + str(edge.name))
        self.assertIsNotNone(edge.tags, "edge attribute 'tags' is None")
        self.assertEqual(edge.tags, {'testKey': 'testValue'},
                         "edge attribute 'sourceKey', Expected: "
                         "" + str({'testKey': 'testValue'}) + ", Actual: " + str(edge.tags))
        edge_json = edge.to_json()
        self.assertEqual(edge_json, '{"fromNodeId": 0, "name": "testName", "edgeId": null, '
                                    '"tags": {"testKey": "testValue"}, "class": "Edge", '
                                    '"toNodeId": 1, "sourceKey": "testSourceKey"}')

    def test_graph_attr(self):
        graph = ground.Graph('testSourceKey', 'testName', {'testKey': 'testValue'})
        self.assertIsNotNone(graph.sourceKey, "graph attribute 'sourceKey' is None")
        self.assertEqual(graph.sourceKey, 'testSourceKey', "graph attribute 'sourceKey', "
                                                           "Expected: testSourceKey, Actual: " + str(graph.sourceKey))
        self.assertIsNotNone(graph.name, "graph attribute 'name' is None")
        self.assertEqual(graph.name, 'testName', "graph attribute 'name', Expected: testName, "
                                                 "Actual: " + str(graph.name))
        self.assertIsNotNone(graph.tags, "graph attribute 'tags' is None")
        self.assertEqual(graph.tags, {'testKey': 'testValue'},
                         "graph attribute 'sourceKey', Expected: "
                         "" + str({'testKey': 'testValue'}) + ", Actual: " + str(graph.tags))
        self.assertEqual(graph.nodes, {})
        self.assertEqual(graph.nodeVersions, {})
        self.assertEqual(graph.edges, {})
        self.assertEqual(graph.edgeVersions, {})
        self.assertEqual(graph.graphs, {})
        self.assertEqual(graph.graphVersions, {})
        self.assertEqual(graph.structures, {})
        self.assertEqual(graph.structureVersions, {})
        self.assertEqual(graph.lineageEdges, {})
        self.assertEqual(graph.lineageEdgeVersions, {})
        self.assertEqual(graph.lineageGraphs, {})
        self.assertEqual(graph.lineageGraphVersions, {})
        self.assertEqual(graph.ids, set([]))
        for i in range(100):
            testId = graph.gen_id()
            self.assertIn(testId, graph.ids)
            self.assertNotIn(len(graph.ids), graph.ids)
        graph_json = graph.to_json()
        self.assertEqual(graph_json, '{"class": "Graph", "graphId": null, "sourceKey": "testSourceKey", '
                                     '"name": "testName", "tags": {"testKey": "testValue"}}')

    def test_structure_attr(self):
        structure = ground.Structure('testSourceKey', 'testName', {'testKey': 'testValue'})
        self.assertIsNotNone(structure.sourceKey, "structure attribute 'sourceKey' is None")
        self.assertEqual(structure.sourceKey, 'testSourceKey', "structure attribute 'sourceKey', "
                                                               "Expected: testSourceKey, "
                                                               "Actual: " + str(structure.sourceKey))
        self.assertIsNotNone(structure.name, "structure attribute 'name' is None")
        self.assertEqual(structure.name, 'testName', "structure attribute 'name', Expected: testName, "
                                                     "Actual: " + str(structure.name))
        self.assertIsNotNone(structure.tags, "structure attribute 'tags' is None")
        self.assertEqual(structure.tags, {'testKey': 'testValue'},
                         "structure attribute 'sourceKey', Expected: "
                         "" + str({'testKey': 'testValue'}) + ", Actual: " + str(structure.tags))
        structure_json = structure.to_json()
        self.assertEqual(structure_json, '{"class": "Structure", "structureId": null, "sourceKey": "testSourceKey", '
                                         '"name": "testName", "tags": {"testKey": "testValue"}}')

    def test_lineage_edge_attr(self):
        lineage_edge = ground.LineageEdge('testSourceKey', 'testName', {'testKey': 'testValue'})
        self.assertIsNotNone(lineage_edge.sourceKey, "lineage_edge attribute 'sourceKey' is None")
        self.assertEqual(lineage_edge.sourceKey, 'testSourceKey', "lineage_edge attribute 'sourceKey', "
                                                                  "Expected: testSourceKey, "
                                                                  "Actual: " + str(lineage_edge.sourceKey))
        self.assertIsNotNone(lineage_edge.name, "lineage_edge attribute 'name' is None")
        self.assertEqual(lineage_edge.name, 'testName', "lineage_edge attribute 'name', Expected: testName, "
                                                        "Actual: " + str(lineage_edge.name))
        self.assertIsNotNone(lineage_edge.tags, "lineage_edge attribute 'tags' is None")
        self.assertEqual(lineage_edge.tags, {'testKey': 'testValue'},
                         "lineage_edge attribute 'sourceKey', Expected: "
                         "" + str({'testKey': 'testValue'}) + ", Actual: " + str(lineage_edge.tags))
        lineage_edge_json = lineage_edge.to_json()
        self.assertEqual(lineage_edge_json, '{"class": "LineageEdge", "tags": {"testKey": "testValue"}, '
                                            '"sourceKey": "testSourceKey", "lineageEdgeId": null, "name": "testName"}')

    def test_lineage_graph_attr(self):
        lineage_graph = ground.LineageGraph('testSourceKey', 'testName', {'testKey': 'testValue'})
        self.assertIsNotNone(lineage_graph.sourceKey, "lineage_graph attribute 'sourceKey' is None")
        self.assertEqual(lineage_graph.sourceKey, 'testSourceKey', "lineage_graph attribute 'sourceKey', "
                                                                   "Expected: testSourceKey, "
                                                                   "Actual: " + str(lineage_graph.sourceKey))
        self.assertIsNotNone(lineage_graph.name, "lineage_graph attribute 'name' is None")
        self.assertEqual(lineage_graph.name, 'testName', "lineage_graph attribute 'name', Expected: testName, "
                                                         "Actual: " + str(lineage_graph.name))
        self.assertIsNotNone(lineage_graph.tags, "lineage_graph attribute 'tags' is None")
        self.assertEqual(lineage_graph.tags, {'testKey': 'testValue'},
                         "lineage_graph attribute 'sourceKey', Expected: "
                         "" + str({'testKey': 'testValue'}) + ", Actual: " + str(lineage_graph.tags))
        lineage_graph_json = lineage_graph.to_json()
        self.assertEqual(lineage_graph_json, '{"lineageGraphId": null, "class": "LineageGraph", '
                                             '"tags": {"testKey": "testValue"}, '
                                             '"sourceKey": "testSourceKey", "name": "testName"}')

    def test_node_minimum(self):
        node = ground.Node('testSourceKey')
        self.assertIsNotNone(node.sourceKey, "node attribute 'sourceKey' is None")
        self.assertEqual(node.sourceKey, 'testSourceKey', "node attribute 'sourceKey', "
                                                          "Expected: testSourceKey, Actual: " + str(node.sourceKey))
        self.assertIsNone(node.name, "node attribute 'name' is not None, Value: " + str(node.name))
        self.assertIsNone(node.tags, "node attribute 'tags' is not None, Value: " + str(node.tags))

    def test_edge_minimum(self):
        edge = ground.Edge('testSourceKey', 0, 1)
        self.assertIsNotNone(edge.sourceKey, "edge attribute 'sourceKey' is None")
        self.assertEqual(edge.sourceKey, 'testSourceKey', "edge attribute 'sourceKey', "
                                                          "Expected: testSourceKey, Actual: " + str(edge.sourceKey))
        self.assertIsNotNone(edge.fromNodeId, "edge attribute 'fromNodeId' is None")
        self.assertEqual(edge.fromNodeId, 0, "edge attribute 'fromNodeId', Expected: 0, "
                                             "Actual: " + str(edge.fromNodeId))
        self.assertIsNotNone(edge.toNodeId, "edge attribute 'toNodeId' is None")
        self.assertEqual(edge.toNodeId, 1, "edge attribute 'toNodeId', Expected: 1, "
                                           "Actual: " + str(edge.toNodeId))
        self.assertIsNone(edge.name, "edge attribute 'name' is not None, Value: " + str(edge.name))
        self.assertIsNone(edge.tags, "edge attribute 'tags' is not None, Value: " + str(edge.tags))

    def test_graph_minimum(self):
        graph = ground.Graph('testSourceKey')
        self.assertIsNotNone(graph.sourceKey, "graph attribute 'sourceKey' is None")
        self.assertEqual(graph.sourceKey, 'testSourceKey', "graph attribute 'sourceKey', "
                                                           "Expected: testSourceKey, Actual: " + str(graph.sourceKey))
        self.assertIsNone(graph.name, "graph attribute 'name' is not None, Value: " + str(graph.name))
        self.assertIsNone(graph.tags, "graph attribute 'tags' is not None, Value: " + str(graph.tags))
        self.assertEqual(graph.nodes, {})
        self.assertEqual(graph.nodeVersions, {})
        self.assertEqual(graph.edges, {})
        self.assertEqual(graph.edgeVersions, {})
        self.assertEqual(graph.graphs, {})
        self.assertEqual(graph.graphVersions, {})
        self.assertEqual(graph.structures, {})
        self.assertEqual(graph.structureVersions, {})
        self.assertEqual(graph.lineageEdges, {})
        self.assertEqual(graph.lineageEdgeVersions, {})
        self.assertEqual(graph.lineageGraphs, {})
        self.assertEqual(graph.lineageGraphVersions, {})
        self.assertEqual(graph.ids, set([]))
        for i in range(100):
            testId = graph.gen_id()
            self.assertIn(testId, graph.ids)
            self.assertNotIn(len(graph.ids), graph.ids)

    def test_structure_minimum(self):
        structure = ground.Structure('testSourceKey')
        self.assertIsNotNone(structure.sourceKey, "structure attribute 'sourceKey' is None")
        self.assertEqual(structure.sourceKey, 'testSourceKey', "structure attribute 'sourceKey', "
                                                               "Expected: testSourceKey, "
                                                               "Actual: " + str(structure.sourceKey))
        self.assertIsNone(structure.name, "structure attribute 'name' is not None, Value: " + str(structure.name))
        self.assertIsNone(structure.tags, "structure attribute 'tags' is not None, Value: " + str(structure.tags))

    def test_lineage_edge_minimum(self):
        lineage_edge = ground.LineageEdge('testSourceKey')
        self.assertIsNotNone(lineage_edge.sourceKey, "lineage_edge attribute 'sourceKey' is None")
        self.assertEqual(lineage_edge.sourceKey, 'testSourceKey', "lineage_edge attribute 'sourceKey', "
                                                                  "Expected: testSourceKey, "
                                                                  "Actual: " + str(lineage_edge.sourceKey))
        self.assertIsNone(lineage_edge.name, "lineage_edge attribute 'name' is not None, "
                                             "Value: " + str(lineage_edge.name))
        self.assertIsNone(lineage_edge.tags, "lineage_edge attribute 'tags' is not None, "
                                             "Value: " + str(lineage_edge.tags))

    def test_lineage_graph_minimum(self):
        lineage_graph = ground.LineageGraph('testSourceKey')
        self.assertIsNotNone(lineage_graph.sourceKey, "lineage_graph attribute 'sourceKey' is None")
        self.assertEqual(lineage_graph.sourceKey, 'testSourceKey', "lineage_graph attribute 'sourceKey', "
                                                                   "Expected: testSourceKey, "
                                                                   "Actual: " + str(lineage_graph.sourceKey))
        self.assertIsNone(lineage_graph.name, "lineage_graph attribute 'name' is not None, "
                                              "Value: " + str(lineage_graph.name))
        self.assertIsNone(lineage_graph.tags, "lineage_graph attribute 'tags' is not None, "
                                              "Value: " + str(lineage_graph.tags))

    def test_node_version_attr(self):
        node = ground.Node('testSourceKey', 'testName', {'testKey': 'testValue'})
        node.nodeId = 0
        node_version = ground.NodeVersion(node, "testReference", "testReferenceParameters",
                                          {'testKey': 'testValue'}, 1, [2, 3])
        self.assertIsNotNone(node_version.sourceKey, "node_version attribute 'sourceKey' is None")
        self.assertEqual(node_version.sourceKey, 'testSourceKey', "node_version attribute 'sourceKey', "
                                                                  "Expected: testSourceKey, "
                                                                  "Actual: " + str(node_version.sourceKey))
        self.assertIsNotNone(node_version.nodeId, "node_version attribute 'nodeId' is None")
        self.assertEqual(node_version.nodeId, 0, "node_version attribute 'nodeId', "
                                                 "Expected: 0, Actual: " + str(node_version.nodeId))
        self.assertIsNotNone(node_version.reference, "node_version attribute 'reference' is None")
        self.assertEqual(node_version.reference, "testReference", "node_version attribute 'reference', "
                                                                  "Expected: testReference, "
                                                                  "Actual: " + str(node_version.reference))
        self.assertIsNotNone(node_version.referenceParameters, "node_version attribute 'referenceParameters' is None")
        self.assertEqual(node_version.referenceParameters, "testReferenceParameters", "node_version attribute "
                                                                                      "'referenceParameters', "
                                                                                      "Expected: testReferenceParameters, "
                                                                                      "Actual: " + str(
            node_version.referenceParameters))
        self.assertIsNotNone(node_version.tags, "node_version attribute 'tags' is None")
        self.assertEqual(node_version.tags,
                         {'testKey': 'testValue'},
                         "node_version attribute 'tags', "
                         "Expected: " + str({'testKey': 'testValue'}) + ", Actual: " + str(node_version.tags))
        self.assertIsNotNone(node_version.structureVersionId, "node_version attribute 'structureVersionId' is None")
        self.assertEqual(node_version.structureVersionId, 1, "node_version attribute 'structureVersionId', "
                                                             "Expected: 1, "
                                                             "Actual: " + str(node_version.structureVersionId))
        self.assertIsNotNone(node_version.parentIds, "node_version attribute 'parentIds' is None")
        self.assertEqual(node_version.parentIds, [2, 3], "node_version attribute 'parentIds', "
                                                         "Expected: [2, 3], "
                                                         "Actual: " + str(node_version.parentIds))
        node_version_json = node_version.to_json()
        self.assertEqual(node_version_json, '{"nodeVersionId": null, "reference": "testReference", '
                                            '"tags": {"testKey": "testValue"}, '
                                            '"referenceParameters": "testReferenceParameters", "class": "NodeVersion", '
                                            '"parentIds": [2, 3], "structureVersionId": 1, '
                                            '"sourceKey": "testSourceKey", "nodeId": 0}')

    def test_edge_version_attr(self):
        edge = ground.Edge('testSourceKey', 0, 10, 'testName', {'testKey': 'testValue'})
        edge.edgeId = 0
        edge_version = ground.EdgeVersion(edge, 4, 5, 6, 7, "testReference", "testReferenceParameters",
                                          {'testKey': 'testValue'}, 1, [2, 3])
        self.assertIsNotNone(edge_version.sourceKey, "edge_version attribute 'sourceKey' is None")
        self.assertEqual(edge_version.sourceKey, 'testSourceKey', "edge_version attribute 'sourceKey', "
                                                                  "Expected: testSourceKey, "
                                                                  "Actual: " + str(edge_version.sourceKey))
        self.assertIsNotNone(edge_version.fromNodeId, "edge_version attribute 'fromNodeId' is None")
        self.assertEqual(edge_version.fromNodeId, 0, "edge_version attribute 'fromNodeId', "
                                                     "Expected: 0, Actual: " + str(edge_version.fromNodeId))
        self.assertIsNotNone(edge_version.toNodeId, "edge_version attribute 'toNodeId' is None")
        self.assertEqual(edge_version.toNodeId, 10, "edge_version attribute 'toNodeId', "
                                                    "Expected: 10, Actual: " + str(edge_version.toNodeId))
        self.assertIsNotNone(edge_version.edgeId, "edge_version attribute 'edgeId' is None")
        self.assertEqual(edge_version.edgeId, 0, "edge_version attribute 'edgeId', "
                                                 "Expected: 0, Actual: " + str(edge_version.edgeId))
        self.assertIsNotNone(edge_version.toNodeVersionStartId, "edge_version attribute 'toNodeVersionStartId' is None")
        self.assertEqual(edge_version.toNodeVersionStartId, 4, "edge_version attribute 'toNodeVersionStartId', "
                                                               "Expected: 4, Actual: " + str(
            edge_version.toNodeVersionStartId))
        self.assertIsNotNone(edge_version.fromNodeVersionStartId, "edge_version attribute 'fromNodeVersionStartId' "
                                                                  "is None")
        self.assertEqual(edge_version.fromNodeVersionStartId, 5, "edge_version attribute 'fromNodeVersionStartId', "
                                                                 "Expected: 5, Actual: " + str(
            edge_version.fromNodeVersionStartId))
        self.assertIsNotNone(edge_version.toNodeVersionEndId, "edge_version attribute 'toNodeVersionEndId' is None")
        self.assertEqual(edge_version.toNodeVersionEndId, 6, "edge_version attribute 'toNodeVersionEndId', "
                                                             "Expected: 6, Actual: " + str(
            edge_version.toNodeVersionEndId))
        self.assertIsNotNone(edge_version.fromNodeVersionEndId, "edge_version attribute 'fromNodeVersionEndId' is None")
        self.assertEqual(edge_version.fromNodeVersionEndId, 7, "edge_version attribute 'fromNodeVersionEndId', "
                                                               "Expected: 7, Actual: " + str(
            edge_version.fromNodeVersionEndId))
        self.assertIsNotNone(edge_version.reference, "edge_version attribute 'reference' is None")
        self.assertEqual(edge_version.reference, "testReference", "edge_version attribute 'reference', "
                                                                  "Expected: testReference, "
                                                                  "Actual: " + str(edge_version.reference))
        self.assertIsNotNone(edge_version.referenceParameters, "edge_version attribute 'referenceParameters' is None")
        self.assertEqual(edge_version.referenceParameters, "testReferenceParameters", "edge_version attribute "
                                                                                      "'referenceParameters', "
                                                                                      "Expected: testReferenceParameters, "
                                                                                      "Actual: " + str(
            edge_version.referenceParameters))
        self.assertIsNotNone(edge_version.tags, "edge_version attribute 'tags' is None")
        self.assertEqual(edge_version.tags,
                         {'testKey': 'testValue'},
                         "edge_version attribute 'tags', "
                         "Expected: " + str({'testKey': 'testValue'}) + ", Actual: " + str(edge_version.tags))
        self.assertIsNotNone(edge_version.structureVersionId, "edge_version attribute 'structureVersionId' is None")
        self.assertEqual(edge_version.structureVersionId, 1, "edge_version attribute 'structureVersionId', "
                                                             "Expected: 1, "
                                                             "Actual: " + str(edge_version.structureVersionId))
        self.assertIsNotNone(edge_version.parentIds, "edge_version attribute 'parentIds' is None")
        self.assertEqual(edge_version.parentIds, [2, 3], "edge_version attribute 'parentIds', "
                                                         "Expected: [2, 3], "
                                                         "Actual: " + str(edge_version.parentIds))
        edge_version_json = edge_version.to_json()
        self.assertEqual(edge_version_json, '{"toNodeVersionStartId": 4, "toNodeVersionEndId": 6, '
                                            '"reference": "testReference", "tags": {"testKey": "testValue"}, '
                                            '"edgeVersionId": null, "referenceParameters": "testReferenceParameters", '
                                            '"class": "EdgeVersion", "fromNodeId": 0, "edgeId": 0, '
                                            '"parentIds": [2, 3], "structureVersionId": 1, '
                                            '"fromNodeVersionStartId": 5, "toNodeId": 10, '
                                            '"fromNodeVersionEndId": 7, "sourceKey": "testSourceKey"}')

    def test_graph_version_attr(self):
        graph = ground.Graph('testSourceKey', 'testName', {'testKey': 'testValue'})
        graph.graphId = 0
        graph_version = ground.GraphVersion(graph, [4, 5, 6], "testReference", "testReferenceParameters",
                                            {'testKey': 'testValue'}, 1, [2, 3])
        self.assertIsNotNone(graph_version.sourceKey, "graph_version attribute 'sourceKey' is None")
        self.assertEqual(graph_version.sourceKey, 'testSourceKey', "graph_version attribute 'sourceKey', "
                                                                   "Expected: testSourceKey, "
                                                                   "Actual: " + str(graph_version.sourceKey))
        self.assertIsNotNone(graph_version.graphId, "graph_version attribute 'graphId' is None")
        self.assertEqual(graph_version.graphId, 0, "graph_version attribute 'graphId', "
                                                   "Expected: 0, Actual: " + str(graph_version.graphId))
        self.assertIsNotNone(graph_version.edgeVersionIds, "graph_version attribute 'edgeVersionIds' is None")
        self.assertEqual(graph_version.edgeVersionIds, [4, 5, 6], "graph_version attribute 'edgeVersionIds', "
                                                                  "Expected: [4, 5, 6], "
                                                                  "Actual: " + str(graph_version.edgeVersionIds))
        self.assertIsNotNone(graph_version.reference, "graph_version attribute 'reference' is None")
        self.assertEqual(graph_version.reference, "testReference", "graph_version attribute 'reference', "
                                                                   "Expected: testReference, "
                                                                   "Actual: " + str(graph_version.reference))
        self.assertIsNotNone(graph_version.referenceParameters, "graph_version attribute 'referenceParameters' is None")
        self.assertEqual(graph_version.referenceParameters, "testReferenceParameters", "graph_version attribute "
                                                                                       "'referenceParameters', "
                                                                                       "Expected: testReferenceParameters, "
                                                                                       "Actual: " + str(
            graph_version.referenceParameters))
        self.assertIsNotNone(graph_version.tags, "graph_version attribute 'tags' is None")
        self.assertEqual(graph_version.tags,
                         {'testKey': 'testValue'},
                         "graph_version attribute 'tags', "
                         "Expected: " + str({'testKey': 'testValue'}) + ", Actual: " + str(graph_version.tags))
        self.assertIsNotNone(graph_version.structureVersionId, "graph_version attribute 'structureVersionId' is None")
        self.assertEqual(graph_version.structureVersionId, 1, "graph_version attribute 'structureVersionId', "
                                                              "Expected: 1, "
                                                              "Actual: " + str(graph_version.structureVersionId))
        self.assertIsNotNone(graph_version.parentIds, "graph_version attribute 'parentIds' is None")
        self.assertEqual(graph_version.parentIds, [2, 3], "graph_version attribute 'parentIds', "
                                                          "Expected: [2, 3], "
                                                          "Actual: " + str(graph_version.parentIds))
        graph_version_json = graph_version.to_json()
        self.assertEqual(graph_version_json, '{"parentIds": [2, 3], "graphId": 0, "reference": "testReference", '
                                             '"edgeVersionIds": [4, 5, 6], '
                                             '"referenceParameters": "testReferenceParameters", '
                                             '"graphVersionId": null, "tags": {"testKey": "testValue"}, '
                                             '"structureVersionId": 1, "sourceKey": "testSourceKey", '
                                             '"class": "GraphVersion"}')

    def test_structure_version_attr(self):
        structure = ground.Structure('testSourceKey', 'testName', {'testKey': 'testValue'})
        structure.structureId = 0
        structure_version = ground.StructureVersion(structure, {'testKey': 'testValue'}, [2, 3])
        self.assertIsNotNone(structure_version.sourceKey, "structure_version attribute 'sourceKey' is None")
        self.assertEqual(structure_version.sourceKey, 'testSourceKey', "structure_version attribute 'sourceKey', "
                                                                       "Expected: testSourceKey, "
                                                                       "Actual: " + str(structure_version.sourceKey))
        self.assertIsNotNone(structure_version.structureId, "structure_version attribute 'structureId' is None")
        self.assertEqual(structure_version.structureId, 0, "structure_version attribute 'structureId', "
                                                           "Expected: 0, Actual: " + str(structure_version.structureId))
        self.assertIsNotNone(structure_version.attributes, "structure_version attribute 'attributes' is None")
        self.assertEqual(structure_version.attributes, {'testKey': 'testValue'}, "structure_version "
                                                                                 "attribute 'attributes', "
                                                                                 "Expected: , " + str(
            {'testKey': 'testValue'}) +
                         ", Actual: " + str(structure_version.attributes))
        self.assertIsNotNone(structure_version.parentIds, "structure_version attribute 'parentIds' is None")
        self.assertEqual(structure_version.parentIds, [2, 3], "structure_version attribute 'parentIds', "
                                                              "Expected: [2, 3], "
                                                              "Actual: " + str(structure_version.parentIds))
        structure_version_json = structure_version.to_json()
        self.assertEqual(structure_version_json, '{"parentIds": [2, 3], "structureId": 0, "structureVersionId": null, '
                                                 '"sourceKey": "testSourceKey", '
                                                 '"attributes": {"testKey": "testValue"}, '
                                                 '"class": "StructureVersion"}')

    def test_lineage_edge_version_attr(self):
        lineage_edge = ground.LineageEdge('testSourceKey', 'testName', {'testKey': 'testValue'})
        lineage_edge.lineageEdgeId = 0
        lineage_edge_version = ground.LineageEdgeVersion(lineage_edge, 5, 4, "testReference", "testReferenceParameters",
                                                         {'testKey': 'testValue'}, 1, [2, 3])
        self.assertIsNotNone(lineage_edge_version.sourceKey, "lineage_edge_version attribute 'sourceKey' is None")
        self.assertEqual(lineage_edge_version.sourceKey, 'testSourceKey', "lineage_edge_version attribute 'sourceKey', "
                                                                          "Expected: testSourceKey, "
                                                                          "Actual: " + str(
            lineage_edge_version.sourceKey))
        self.assertIsNotNone(lineage_edge_version.lineageEdgeId, "lineage_edge_version attribute "
                                                                 "'lineageEdgeId' is None")
        self.assertEqual(lineage_edge_version.lineageEdgeId, 0, "lineage_edge_version attribute 'lineageEdgeId', "
                                                                "Expected: 0, Actual: " + str(
            lineage_edge_version.lineageEdgeId))
        self.assertIsNotNone(lineage_edge_version.fromRichVersionId, "lineage_edge_version attribute "
                                                                     "'fromRichVersionId' is None")
        self.assertEqual(lineage_edge_version.fromRichVersionId, 4,
                         "lineage_edge_version attribute 'fromRichVersionId', "
                         "Expected: 4, Actual: " + str(lineage_edge_version.fromRichVersionId))
        self.assertIsNotNone(lineage_edge_version.toRichVersionId, "lineage_edge_version attribute "
                                                                   "'toRichVersionId' is None")
        self.assertEqual(lineage_edge_version.toRichVersionId, 5, "lineage_edge_version attribute 'toRichVersionId', "
                                                                  "Expected: 5, Actual: " + str(
            lineage_edge_version.toRichVersionId))
        self.assertIsNotNone(lineage_edge_version.reference, "lineage_edge_version attribute 'reference' is None")
        self.assertEqual(lineage_edge_version.reference, "testReference", "lineage_edge_version attribute 'reference', "
                                                                          "Expected: testReference, "
                                                                          "Actual: " + str(
            lineage_edge_version.reference))
        self.assertIsNotNone(lineage_edge_version.referenceParameters, "lineage_edge_version attribute "
                                                                       "'referenceParameters' is None")
        self.assertEqual(lineage_edge_version.referenceParameters, "testReferenceParameters",
                         "lineage_edge_version "
                         "attribute "
                         "'referenceParameters', "
                         "Expected: testReferenceParameters, "
                         "Actual: " + str(lineage_edge_version.referenceParameters))
        self.assertIsNotNone(lineage_edge_version.tags, "lineage_edge_version attribute 'tags' is None")
        self.assertEqual(lineage_edge_version.tags,
                         {'testKey': 'testValue'},
                         "lineage_edge_version attribute 'tags', "
                         "Expected: " + str({'testKey': 'testValue'}) + ", Actual: " + str(lineage_edge_version.tags))
        self.assertIsNotNone(lineage_edge_version.structureVersionId,
                             "lineage_edge_version attribute 'structureVersionId' is None")
        self.assertEqual(lineage_edge_version.structureVersionId, 1,
                         "lineage_edge_version attribute 'structureVersionId', "
                         "Expected: 1, "
                         "Actual: " + str(lineage_edge_version.structureVersionId))
        self.assertIsNotNone(lineage_edge_version.parentIds, "lineage_edge_version attribute 'parentIds' is None")
        self.assertEqual(lineage_edge_version.parentIds, [2, 3], "lineage_edge_version attribute 'parentIds', "
                                                                 "Expected: [2, 3], "
                                                                 "Actual: " + str(lineage_edge_version.parentIds))
        lineage_edge_version_json = lineage_edge_version.to_json()
        self.assertEqual(lineage_edge_version_json, '{"reference": "testReference", "tags": {"testKey": "testValue"}, '
                                                    '"lineageEdgeVersionId": null, '
                                                    '"referenceParameters": "testReferenceParameters", '
                                                    '"fromRichVersionId": 4, "class": "LineageEdgeVersion", '
                                                    '"parentIds": [2, 3], "structureVersionId": 1, '
                                                    '"toRichVersionId": 5, "lineageEdgeId": 0, '
                                                    '"sourceKey": "testSourceKey"}')

    def test_lineage_graph_version_attr(self):
        lineage_graph = ground.LineageGraph('testSourceKey', 'testName', {'testKey': 'testValue'})
        lineage_graph.lineageGraphId = 0
        lineage_graph_version = ground.LineageGraphVersion(lineage_graph, [5, 4], "testReference",
                                                           "testReferenceParameters",
                                                           {'testKey': 'testValue'}, 1, [2, 3])
        self.assertIsNotNone(lineage_graph_version.sourceKey, "lineage_graph_version attribute 'sourceKey' is None")
        self.assertEqual(lineage_graph_version.sourceKey, 'testSourceKey',
                         "lineage_graph_version attribute 'sourceKey', "
                         "Expected: testSourceKey, "
                         "Actual: " + str(
                             lineage_graph_version.sourceKey))
        self.assertIsNotNone(lineage_graph_version.lineageGraphId, "lineage_graph_version attribute "
                                                                   "'lineageGraphId' is None")
        self.assertEqual(lineage_graph_version.lineageGraphId, 0, "lineage_graph_version attribute 'lineageGraphId', "
                                                                  "Expected: 0, Actual: " + str(
            lineage_graph_version.lineageGraphId))
        self.assertIsNotNone(lineage_graph_version.lineageEdgeVersionIds, "lineage_graph_version attribute "
                                                                          "'lineageEdgeVersionIds' is None")
        self.assertEqual(lineage_graph_version.lineageEdgeVersionIds, [5, 4],
                         "lineage_graph_version attribute 'lineageEdgeVersionIds', "
                         "Expected: [5, 4], Actual: " + str(lineage_graph_version.lineageEdgeVersionIds))
        self.assertIsNotNone(lineage_graph_version.reference, "lineage_graph_version attribute 'reference' is None")
        self.assertEqual(lineage_graph_version.reference, "testReference",
                         "lineage_graph_version attribute 'reference', "
                         "Expected: testReference, "
                         "Actual: " + str(
                             lineage_graph_version.reference))
        self.assertIsNotNone(lineage_graph_version.referenceParameters, "lineage_graph_version attribute "
                                                                        "'referenceParameters' is None")
        self.assertEqual(lineage_graph_version.referenceParameters, "testReferenceParameters",
                         "lineage_graph_version "
                         "attribute "
                         "'referenceParameters', "
                         "Expected: testReferenceParameters, "
                         "Actual: " + str(lineage_graph_version.referenceParameters))
        self.assertIsNotNone(lineage_graph_version.tags, "lineage_graph_version attribute 'tags' is None")
        self.assertEqual(lineage_graph_version.tags,
                         {'testKey': 'testValue'},
                         "lineage_graph_version attribute 'tags', "
                         "Expected: " + str({'testKey': 'testValue'}) + ", Actual: " + str(lineage_graph_version.tags))
        self.assertIsNotNone(lineage_graph_version.structureVersionId,
                             "lineage_graph_version attribute 'structureVersionId' is None")
        self.assertEqual(lineage_graph_version.structureVersionId, 1,
                         "lineage_graph_version attribute 'structureVersionId', "
                         "Expected: 1, "
                         "Actual: " + str(lineage_graph_version.structureVersionId))
        self.assertIsNotNone(lineage_graph_version.parentIds, "lineage_graph_version attribute 'parentIds' is None")
        self.assertEqual(lineage_graph_version.parentIds, [2, 3], "lineage_graph_version attribute 'parentIds', "
                                                                  "Expected: [2, 3], "
                                                                  "Actual: " + str(lineage_graph_version.parentIds))
        lineage_graph_version_json = lineage_graph_version.to_json()
        self.assertEqual(lineage_graph_version_json, '{"lineageGraphId": 0, "lineageGraphVersionId": null, '
                                                     '"parentIds": [2, 3], "reference": "testReference", '
                                                     '"tags": {"testKey": "testValue"}, '
                                                     '"referenceParameters": "testReferenceParameters", '
                                                     '"lineageEdgeVersionIds": [5, 4], '
                                                     '"structureVersionId": 1, "sourceKey": "testSourceKey", '
                                                     '"class": "LineageGraphVersion"}')

    def test_node_version_minimum(self):
        node = ground.Node('testSourceKey', 'testName', {'testKey': 'testValue'})
        node.nodeId = 0
        node_version = ground.NodeVersion(node)
        self.assertIsNotNone(node_version.sourceKey, "node_version attribute 'sourceKey' is None")
        self.assertEqual(node_version.sourceKey, 'testSourceKey', "node_version attribute 'sourceKey', "
                                                                  "Expected: testSourceKey, "
                                                                  "Actual: " + str(node_version.sourceKey))
        self.assertIsNotNone(node_version.nodeId, "node_version attribute 'nodeId' is None")
        self.assertEqual(node_version.nodeId, 0, "node_version attribute 'nodeId', "
                                                 "Expected: 0, Actual: " + str(node_version.nodeId))
        self.assertIsNone(node_version.reference, "node_version attribute 'reference' is not None")
        self.assertIsNone(node_version.referenceParameters, "node_version attribute 'referenceParameters' is not None")
        self.assertIsNone(node_version.tags, "node_version attribute 'tags' is not None")
        self.assertIsNone(node_version.structureVersionId, "node_version attribute 'structureVersionId' is not None")
        self.assertIsNone(node_version.parentIds, "node_version attribute 'parentIds' is not None")

    def test_edge_version_minimum(self):
        edge = ground.Edge('testSourceKey', 0, 10)
        edge.edgeId = 0
        edge_version = ground.EdgeVersion(edge, 4, 5)
        self.assertIsNotNone(edge_version.sourceKey, "edge_version attribute 'sourceKey' is None")
        self.assertEqual(edge_version.sourceKey, 'testSourceKey', "edge_version attribute 'sourceKey', "
                                                                  "Expected: testSourceKey, "
                                                                  "Actual: " + str(edge_version.sourceKey))
        self.assertIsNotNone(edge_version.fromNodeId, "edge_version attribute 'fromNodeId' is None")
        self.assertEqual(edge_version.fromNodeId, 0, "edge_version attribute 'fromNodeId', "
                                                     "Expected: 0, Actual: " + str(edge_version.fromNodeId))
        self.assertIsNotNone(edge_version.toNodeId, "edge_version attribute 'toNodeId' is None")
        self.assertEqual(edge_version.toNodeId, 10, "edge_version attribute 'toNodeId', "
                                                    "Expected: 10, Actual: " + str(edge_version.toNodeId))
        self.assertIsNotNone(edge_version.edgeId, "edge_version attribute 'edgeId' is None")
        self.assertEqual(edge_version.edgeId, 0, "edge_version attribute 'edgeId', "
                                                 "Expected: 0, Actual: " + str(edge_version.edgeId))
        self.assertIsNotNone(edge_version.toNodeVersionStartId, "edge_version attribute 'toNodeVersionStartId' is None")
        self.assertEqual(edge_version.toNodeVersionStartId, 4, "edge_version attribute 'toNodeVersionStartId', "
                                                               "Expected: 4, Actual: " + str(
            edge_version.toNodeVersionStartId))
        self.assertIsNotNone(edge_version.fromNodeVersionStartId, "edge_version attribute 'fromNodeVersionStartId' "
                                                                  "is None")
        self.assertEqual(edge_version.fromNodeVersionStartId, 5, "edge_version attribute 'fromNodeVersionStartId', "
                                                                 "Expected: 5, Actual: " + str(
            edge_version.fromNodeVersionStartId))
        self.assertIsNone(edge_version.toNodeVersionEndId, "edge_version attribute 'toNodeVersionEndId' is not None")
        self.assertIsNone(edge_version.fromNodeVersionEndId, "edge_version attribute 'fromNodeVersionEndId' "
                                                             "is not None")
        self.assertIsNone(edge_version.reference, "edge_version attribute 'reference' is not None")
        self.assertIsNone(edge_version.referenceParameters, "edge_version attribute 'referenceParameters' is not None")
        self.assertIsNone(edge_version.tags, "edge_version attribute 'tags' is not None")
        self.assertIsNone(edge_version.structureVersionId, "edge_version attribute 'structureVersionId' is not None")
        self.assertIsNone(edge_version.parentIds, "edge_version attribute 'parentIds' is not None")

    def test_graph_version_minimum(self):
        graph = ground.Graph('testSourceKey')
        graph.graphId = 0
        graph_version = ground.GraphVersion(graph, [4, 5, 6])
        self.assertIsNotNone(graph_version.sourceKey, "graph_version attribute 'sourceKey' is None")
        self.assertEqual(graph_version.sourceKey, 'testSourceKey', "graph_version attribute 'sourceKey', "
                                                                   "Expected: testSourceKey, "
                                                                   "Actual: " + str(graph_version.sourceKey))
        self.assertIsNotNone(graph_version.graphId, "graph_version attribute 'graphId' is None")
        self.assertEqual(graph_version.graphId, 0, "graph_version attribute 'graphId', "
                                                   "Expected: 0, Actual: " + str(graph_version.graphId))
        self.assertIsNotNone(graph_version.edgeVersionIds, "graph_version attribute 'edgeVersionIds' is None")
        self.assertEqual(graph_version.edgeVersionIds, [4, 5, 6], "graph_version attribute 'edgeVersionIds', "
                                                                  "Expected: [4, 5, 6], "
                                                                  "Actual: " + str(graph_version.edgeVersionIds))
        self.assertIsNone(graph_version.reference, "graph_version attribute 'reference' is not None")
        self.assertIsNone(graph_version.referenceParameters, "graph_version attribute 'referenceParameters' "
                                                             "is not None")
        self.assertIsNone(graph_version.tags, "graph_version attribute 'tags' is not None")
        self.assertIsNone(graph_version.structureVersionId, "graph_version attribute 'structureVersionId' is not None")
        self.assertIsNone(graph_version.parentIds, "graph_version attribute 'parentIds' is not None")

    def test_structure_version_minimum(self):
        structure = ground.Structure('testSourceKey')
        structure.structureId = 0
        structure_version = ground.StructureVersion(structure, {'testKey': 'testValue'})
        self.assertIsNotNone(structure_version.sourceKey, "structure_version attribute 'sourceKey' is None")
        self.assertEqual(structure_version.sourceKey, 'testSourceKey', "structure_version attribute 'sourceKey', "
                                                                       "Expected: testSourceKey, "
                                                                       "Actual: " + str(structure_version.sourceKey))
        self.assertIsNotNone(structure_version.structureId, "structure_version attribute 'structureId' is None")
        self.assertEqual(structure_version.structureId, 0, "structure_version attribute 'structureId', "
                                                           "Expected: 0, Actual: " + str(structure_version.structureId))
        self.assertIsNotNone(structure_version.attributes, "structure_version attribute 'attributes' is None")
        self.assertEqual(structure_version.attributes, {'testKey': 'testValue'}, "structure_version "
                                                                                 "attribute 'attributes', "
                                                                                 "Expected: , " + str(
            {'testKey': 'testValue'}) +
                         ", Actual: " + str(structure_version.attributes))
        self.assertIsNone(structure_version.parentIds, "structure_version attribute 'parentIds' is not None")

    def test_lineage_edge_version_minimum(self):
        lineage_edge = ground.LineageEdge('testSourceKey')
        lineage_edge.lineageEdgeId = 0
        lineage_edge_version = ground.LineageEdgeVersion(lineage_edge, 5, 4)
        self.assertIsNotNone(lineage_edge_version.sourceKey, "lineage_edge_version attribute 'sourceKey' is None")
        self.assertEqual(lineage_edge_version.sourceKey, 'testSourceKey', "lineage_edge_version attribute 'sourceKey', "
                                                                          "Expected: testSourceKey, "
                                                                          "Actual: " + str(
            lineage_edge_version.sourceKey))
        self.assertIsNotNone(lineage_edge_version.lineageEdgeId, "lineage_edge_version attribute "
                                                                 "'lineageEdgeId' is None")
        self.assertEqual(lineage_edge_version.lineageEdgeId, 0, "lineage_edge_version attribute 'lineageEdgeId', "
                                                                "Expected: 0, Actual: " + str(
            lineage_edge_version.lineageEdgeId))
        self.assertIsNotNone(lineage_edge_version.fromRichVersionId, "lineage_edge_version attribute "
                                                                     "'fromRichVersionId' is None")
        self.assertEqual(lineage_edge_version.fromRichVersionId, 4,
                         "lineage_edge_version attribute 'fromRichVersionId', "
                         "Expected: 4, Actual: " + str(lineage_edge_version.fromRichVersionId))
        self.assertIsNotNone(lineage_edge_version.toRichVersionId, "lineage_edge_version attribute "
                                                                   "'toRichVersionId' is None")
        self.assertEqual(lineage_edge_version.toRichVersionId, 5, "lineage_edge_version attribute 'toRichVersionId', "
                                                                  "Expected: 5, Actual: " + str(
            lineage_edge_version.toRichVersionId))
        self.assertIsNone(lineage_edge_version.reference, "lineage_edge_version attribute 'reference' is not None")
        self.assertIsNone(lineage_edge_version.referenceParameters, "lineage_edge_version attribute "
                                                                    "'referenceParameters' is not None")
        self.assertIsNone(lineage_edge_version.tags, "lineage_edge_version attribute 'tags' is not None")
        self.assertIsNone(lineage_edge_version.structureVersionId,
                          "lineage_edge_version attribute 'structureVersionId' is not None")
        self.assertIsNone(lineage_edge_version.parentIds, "lineage_edge_version attribute 'parentIds' is not None")

    def test_lineage_graph_version_minimum(self):
        lineage_graph = ground.LineageGraph('testSourceKey')
        lineage_graph.lineageGraphId = 0
        lineage_graph_version = ground.LineageGraphVersion(lineage_graph, [5, 4])
        self.assertIsNotNone(lineage_graph_version.sourceKey, "lineage_graph_version attribute 'sourceKey' is None")
        self.assertEqual(lineage_graph_version.sourceKey, 'testSourceKey',
                         "lineage_graph_version attribute 'sourceKey', "
                         "Expected: testSourceKey, "
                         "Actual: " + str(
                             lineage_graph_version.sourceKey))
        self.assertIsNotNone(lineage_graph_version.lineageGraphId, "lineage_graph_version attribute "
                                                                   "'lineageGraphId' is None")
        self.assertEqual(lineage_graph_version.lineageGraphId, 0, "lineage_graph_version attribute 'lineageGraphId', "
                                                                  "Expected: 0, Actual: " + str(
            lineage_graph_version.lineageGraphId))
        self.assertIsNotNone(lineage_graph_version.lineageEdgeVersionIds, "lineage_graph_version attribute "
                                                                          "'lineageEdgeVersionIds' is None")
        self.assertEqual(lineage_graph_version.lineageEdgeVersionIds, [5, 4],
                         "lineage_graph_version attribute 'lineageEdgeVersionIds', "
                         "Expected: [5, 4], Actual: " + str(lineage_graph_version.lineageEdgeVersionIds))
        self.assertIsNone(lineage_graph_version.reference, "lineage_graph_version attribute 'reference' is not None")
        self.assertIsNone(lineage_graph_version.referenceParameters, "lineage_graph_version attribute "
                                                                     "'referenceParameters' is not None")
        self.assertIsNone(lineage_graph_version.tags, "lineage_graph_version attribute 'tags' is not None")
        self.assertIsNone(lineage_graph_version.structureVersionId,
                          "lineage_graph_version attribute 'structureVersionId' is not None")
        self.assertIsNone(lineage_graph_version.parentIds, "lineage_graph_version attribute 'parentIds' is not None")

    def test_git_create_node(self):
        git = ground.GitImplementation()
        nodeId = git.createNode('testSourceKey', 'testName', {'testKey': 'testValue'})
        node = git.graph.nodes[nodeId]
        self.assertIsNotNone(node.sourceKey, "node attribute 'sourceKey' is None")
        self.assertEqual(node.sourceKey, 'testSourceKey', "node attribute 'sourceKey', "
                                                          "Expected: testSourceKey, Actual: " + str(node.sourceKey))
        self.assertIsNotNone(node.name, "node attribute 'name' is None")
        self.assertEqual(node.name, 'testName', "node attribute 'name', Expected: testName, "
                                                "Actual: " + str(node.name))
        self.assertIsNotNone(node.tags, "node attribute 'tags' is None")
        self.assertEqual(node.tags, {'testKey': 'testValue'},
                         "node attribute 'sourceKey', Expected: "
                         "" + str({'testKey': 'testValue'}) + ", Actual: " + str(node.tags))
        node_json = node.to_json()
        self.assertEqual(node_json, '{"nodeId": 0, "class": "Node", "sourceKey": "testSourceKey", '
                                    '"name": "testName", "tags": {"testKey": "testValue"}}')

    def test_git_create_edge(self):
        git = ground.GitImplementation()
        edgeId = git.createEdge('testSourceKey', 0, 1, 'testName', {'testKey': 'testValue'})
        edge = git.graph.edges[edgeId]
        self.assertIsNotNone(edge.sourceKey, "edge attribute 'sourceKey' is None")
        self.assertEqual(edge.sourceKey, 'testSourceKey', "edge attribute 'sourceKey', "
                                                          "Expected: testSourceKey, Actual: " + str(edge.sourceKey))
        self.assertIsNotNone(edge.fromNodeId, "edge attribute 'fromNodeId' is None")
        self.assertEqual(edge.fromNodeId, 0, "edge attribute 'fromNodeId', Expected: 0, "
                                             "Actual: " + str(edge.fromNodeId))
        self.assertIsNotNone(edge.toNodeId, "edge attribute 'toNodeId' is None")
        self.assertEqual(edge.toNodeId, 1, "edge attribute 'toNodeId', Expected: 1, "
                                           "Actual: " + str(edge.toNodeId))
        self.assertIsNotNone(edge.name, "edge attribute 'name' is None")
        self.assertEqual(edge.name, 'testName', "edge attribute 'name', Expected: testName, "
                                                "Actual: " + str(edge.name))
        self.assertIsNotNone(edge.tags, "edge attribute 'tags' is None")
        self.assertEqual(edge.tags, {'testKey': 'testValue'},
                         "edge attribute 'sourceKey', Expected: "
                         "" + str({'testKey': 'testValue'}) + ", Actual: " + str(edge.tags))
        edge_json = edge.to_json()
        self.assertEqual(edge_json, '{"fromNodeId": 0, "name": "testName", "edgeId": 0, '
                                    '"tags": {"testKey": "testValue"}, "class": "Edge", '
                                    '"toNodeId": 1, "sourceKey": "testSourceKey"}')

    def test_git_create_graph(self):
        git = ground.GitImplementation()
        graphId = git.createGraph('testSourceKey', 'testName', {'testKey': 'testValue'})
        graph = git.graph.graphs[graphId]
        self.assertIsNotNone(graph.sourceKey, "graph attribute 'sourceKey' is None")
        self.assertEqual(graph.sourceKey, 'testSourceKey', "graph attribute 'sourceKey', "
                                                           "Expected: testSourceKey, Actual: " + str(graph.sourceKey))
        self.assertIsNotNone(graph.name, "graph attribute 'name' is None")
        self.assertEqual(graph.name, 'testName', "graph attribute 'name', Expected: testName, "
                                                 "Actual: " + str(graph.name))
        self.assertIsNotNone(graph.tags, "graph attribute 'tags' is None")
        self.assertEqual(graph.tags, {'testKey': 'testValue'},
                         "graph attribute 'sourceKey', Expected: "
                         "" + str({'testKey': 'testValue'}) + ", Actual: " + str(graph.tags))
        self.assertEqual(graph.nodes, {})
        self.assertEqual(graph.nodeVersions, {})
        self.assertEqual(graph.edges, {})
        self.assertEqual(graph.edgeVersions, {})
        self.assertEqual(graph.graphs, {})
        self.assertEqual(graph.graphVersions, {})
        self.assertEqual(graph.structures, {})
        self.assertEqual(graph.structureVersions, {})
        self.assertEqual(graph.lineageEdges, {})
        self.assertEqual(graph.lineageEdgeVersions, {})
        self.assertEqual(graph.lineageGraphs, {})
        self.assertEqual(graph.lineageGraphVersions, {})
        self.assertEqual(graph.ids, set([]))
        for i in range(100):
            testId = graph.gen_id()
            self.assertIn(testId, graph.ids)
            self.assertNotIn(len(graph.ids), graph.ids)
        graph_json = graph.to_json()
        self.assertEqual(graph_json, '{"class": "Graph", "graphId": 0, "sourceKey": "testSourceKey", '
                                     '"name": "testName", "tags": {"testKey": "testValue"}}')

    def test_git_create_structure(self):
        git = ground.GitImplementation()
        structureId = git.createStructure('testSourceKey', 'testName', {'testKey': 'testValue'})
        structure = git.graph.structures[structureId]
        self.assertIsNotNone(structure.sourceKey, "structure attribute 'sourceKey' is None")
        self.assertEqual(structure.sourceKey, 'testSourceKey', "structure attribute 'sourceKey', "
                                                               "Expected: testSourceKey, "
                                                               "Actual: " + str(structure.sourceKey))
        self.assertIsNotNone(structure.name, "structure attribute 'name' is None")
        self.assertEqual(structure.name, 'testName', "structure attribute 'name', Expected: testName, "
                                                     "Actual: " + str(structure.name))
        self.assertIsNotNone(structure.tags, "structure attribute 'tags' is None")
        self.assertEqual(structure.tags, {'testKey': 'testValue'},
                         "structure attribute 'sourceKey', Expected: "
                         "" + str({'testKey': 'testValue'}) + ", Actual: " + str(structure.tags))
        structure_json = structure.to_json()
        self.assertEqual(structure_json, '{"class": "Structure", "structureId": 0, "sourceKey": "testSourceKey", '
                                         '"name": "testName", "tags": {"testKey": "testValue"}}')

    def test_git_create_lineage_edge(self):
        git = ground.GitImplementation()
        lineageEdgeId = git.createLineageEdge('testSourceKey', 'testName', {'testKey': 'testValue'})
        lineage_edge = git.graph.lineageEdges[lineageEdgeId]
        self.assertIsNotNone(lineage_edge.sourceKey, "lineage_edge attribute 'sourceKey' is None")
        self.assertEqual(lineage_edge.sourceKey, 'testSourceKey', "lineage_edge attribute 'sourceKey', "
                                                                  "Expected: testSourceKey, "
                                                                  "Actual: " + str(lineage_edge.sourceKey))
        self.assertIsNotNone(lineage_edge.name, "lineage_edge attribute 'name' is None")
        self.assertEqual(lineage_edge.name, 'testName', "lineage_edge attribute 'name', Expected: testName, "
                                                        "Actual: " + str(lineage_edge.name))
        self.assertIsNotNone(lineage_edge.tags, "lineage_edge attribute 'tags' is None")
        self.assertEqual(lineage_edge.tags, {'testKey': 'testValue'},
                         "lineage_edge attribute 'sourceKey', Expected: "
                         "" + str({'testKey': 'testValue'}) + ", Actual: " + str(lineage_edge.tags))
        lineage_edge_json = lineage_edge.to_json()
        self.assertEqual(lineage_edge_json, '{"class": "LineageEdge", "tags": {"testKey": "testValue"}, '
                                            '"sourceKey": "testSourceKey", "lineageEdgeId": 0, "name": "testName"}')

    def test_git_create_lineage_graph(self):
        git = ground.GitImplementation()
        lineageGraphId = git.createLineageGraph('testSourceKey', 'testName', {'testKey': 'testValue'})
        lineage_graph = git.graph.lineageGraphs[lineageGraphId]
        self.assertIsNotNone(lineage_graph.sourceKey, "lineage_graph attribute 'sourceKey' is None")
        self.assertEqual(lineage_graph.sourceKey, 'testSourceKey', "lineage_graph attribute 'sourceKey', "
                                                                   "Expected: testSourceKey, "
                                                                   "Actual: " + str(lineage_graph.sourceKey))
        self.assertIsNotNone(lineage_graph.name, "lineage_graph attribute 'name' is None")
        self.assertEqual(lineage_graph.name, 'testName', "lineage_graph attribute 'name', Expected: testName, "
                                                         "Actual: " + str(lineage_graph.name))
        self.assertIsNotNone(lineage_graph.tags, "lineage_graph attribute 'tags' is None")
        self.assertEqual(lineage_graph.tags, {'testKey': 'testValue'},
                         "lineage_graph attribute 'sourceKey', Expected: "
                         "" + str({'testKey': 'testValue'}) + ", Actual: " + str(lineage_graph.tags))
        lineage_graph_json = lineage_graph.to_json()
        self.assertEqual(lineage_graph_json, '{"lineageGraphId": 0, "class": "LineageGraph", '
                                             '"tags": {"testKey": "testValue"}, '
                                             '"sourceKey": "testSourceKey", "name": "testName"}')

    def test_git_get_node(self):
        git = ground.GitImplementation()
        nodeId = git.createNode('testSourceKey', 'testName', {'testKey': 'testValue'})
        node = git.getNode('testSourceKey')
        self.assertIsNotNone(node.sourceKey, "node attribute 'sourceKey' is None")
        self.assertEqual(node.sourceKey, 'testSourceKey', "node attribute 'sourceKey', "
                                                          "Expected: testSourceKey, Actual: " + str(node.sourceKey))
        self.assertIsNotNone(node.name, "node attribute 'name' is None")
        self.assertEqual(node.name, 'testName', "node attribute 'name', Expected: testName, "
                                                "Actual: " + str(node.name))
        self.assertIsNotNone(node.tags, "node attribute 'tags' is None")
        self.assertEqual(node.tags, {'testKey': 'testValue'},
                         "node attribute 'sourceKey', Expected: "
                         "" + str({'testKey': 'testValue'}) + ", Actual: " + str(node.tags))
        node_json = node.to_json()
        self.assertEqual(node_json, '{"nodeId": 0, "class": "Node", "sourceKey": "testSourceKey", '
                                    '"name": "testName", "tags": {"testKey": "testValue"}}')

    def test_git_get_edge(self):
        git = ground.GitImplementation()
        edgeId = git.createEdge('testSourceKey', 0, 1, 'testName', {'testKey': 'testValue'})
        edge = git.getEdge('testSourceKey')
        self.assertIsNotNone(edge.sourceKey, "edge attribute 'sourceKey' is None")
        self.assertEqual(edge.sourceKey, 'testSourceKey', "edge attribute 'sourceKey', "
                                                          "Expected: testSourceKey, Actual: " + str(edge.sourceKey))
        self.assertIsNotNone(edge.fromNodeId, "edge attribute 'fromNodeId' is None")
        self.assertEqual(edge.fromNodeId, 0, "edge attribute 'fromNodeId', Expected: 0, "
                                             "Actual: " + str(edge.fromNodeId))
        self.assertIsNotNone(edge.toNodeId, "edge attribute 'toNodeId' is None")
        self.assertEqual(edge.toNodeId, 1, "edge attribute 'toNodeId', Expected: 1, "
                                           "Actual: " + str(edge.toNodeId))
        self.assertIsNotNone(edge.name, "edge attribute 'name' is None")
        self.assertEqual(edge.name, 'testName', "edge attribute 'name', Expected: testName, "
                                                "Actual: " + str(edge.name))
        self.assertIsNotNone(edge.tags, "edge attribute 'tags' is None")
        self.assertEqual(edge.tags, {'testKey': 'testValue'},
                         "edge attribute 'sourceKey', Expected: "
                         "" + str({'testKey': 'testValue'}) + ", Actual: " + str(edge.tags))
        edge_json = edge.to_json()
        self.assertEqual(edge_json, '{"fromNodeId": 0, "name": "testName", "edgeId": 0, '
                                    '"tags": {"testKey": "testValue"}, "class": "Edge", '
                                    '"toNodeId": 1, "sourceKey": "testSourceKey"}')

    def test_git_get_graph(self):
        git = ground.GitImplementation()
        graphId = git.createGraph('testSourceKey', 'testName', {'testKey': 'testValue'})
        graph = git.getGraph('testSourceKey')
        self.assertIsNotNone(graph.sourceKey, "graph attribute 'sourceKey' is None")
        self.assertEqual(graph.sourceKey, 'testSourceKey', "graph attribute 'sourceKey', "
                                                           "Expected: testSourceKey, Actual: " + str(graph.sourceKey))
        self.assertIsNotNone(graph.name, "graph attribute 'name' is None")
        self.assertEqual(graph.name, 'testName', "graph attribute 'name', Expected: testName, "
                                                 "Actual: " + str(graph.name))
        self.assertIsNotNone(graph.tags, "graph attribute 'tags' is None")
        self.assertEqual(graph.tags, {'testKey': 'testValue'},
                         "graph attribute 'sourceKey', Expected: "
                         "" + str({'testKey': 'testValue'}) + ", Actual: " + str(graph.tags))
        self.assertEqual(graph.nodes, {})
        self.assertEqual(graph.nodeVersions, {})
        self.assertEqual(graph.edges, {})
        self.assertEqual(graph.edgeVersions, {})
        self.assertEqual(graph.graphs, {})
        self.assertEqual(graph.graphVersions, {})
        self.assertEqual(graph.structures, {})
        self.assertEqual(graph.structureVersions, {})
        self.assertEqual(graph.lineageEdges, {})
        self.assertEqual(graph.lineageEdgeVersions, {})
        self.assertEqual(graph.lineageGraphs, {})
        self.assertEqual(graph.lineageGraphVersions, {})
        self.assertEqual(graph.ids, set([]))
        for i in range(100):
            testId = graph.gen_id()
            self.assertIn(testId, graph.ids)
            self.assertNotIn(len(graph.ids), graph.ids)
        graph_json = graph.to_json()
        self.assertEqual(graph_json, '{"class": "Graph", "graphId": 0, "sourceKey": "testSourceKey", '
                                     '"name": "testName", "tags": {"testKey": "testValue"}}')

    def test_git_get_structure(self):
        git = ground.GitImplementation()
        structureId = git.createStructure('testSourceKey', 'testName', {'testKey': 'testValue'})
        structure = git.getStructure('testSourceKey')
        self.assertIsNotNone(structure.sourceKey, "structure attribute 'sourceKey' is None")
        self.assertEqual(structure.sourceKey, 'testSourceKey', "structure attribute 'sourceKey', "
                                                               "Expected: testSourceKey, "
                                                               "Actual: " + str(structure.sourceKey))
        self.assertIsNotNone(structure.name, "structure attribute 'name' is None")
        self.assertEqual(structure.name, 'testName', "structure attribute 'name', Expected: testName, "
                                                     "Actual: " + str(structure.name))
        self.assertIsNotNone(structure.tags, "structure attribute 'tags' is None")
        self.assertEqual(structure.tags, {'testKey': 'testValue'},
                         "structure attribute 'sourceKey', Expected: "
                         "" + str({'testKey': 'testValue'}) + ", Actual: " + str(structure.tags))
        structure_json = structure.to_json()
        self.assertEqual(structure_json, '{"class": "Structure", "structureId": 0, "sourceKey": "testSourceKey", '
                                         '"name": "testName", "tags": {"testKey": "testValue"}}')

    def test_git_get_lineage_edge(self):
        git = ground.GitImplementation()
        lineageEdgeId = git.createLineageEdge('testSourceKey', 'testName', {'testKey': 'testValue'})
        lineage_edge = git.getLineageEdge('testSourceKey')
        self.assertIsNotNone(lineage_edge.sourceKey, "lineage_edge attribute 'sourceKey' is None")
        self.assertEqual(lineage_edge.sourceKey, 'testSourceKey', "lineage_edge attribute 'sourceKey', "
                                                                  "Expected: testSourceKey, "
                                                                  "Actual: " + str(lineage_edge.sourceKey))
        self.assertIsNotNone(lineage_edge.name, "lineage_edge attribute 'name' is None")
        self.assertEqual(lineage_edge.name, 'testName', "lineage_edge attribute 'name', Expected: testName, "
                                                        "Actual: " + str(lineage_edge.name))
        self.assertIsNotNone(lineage_edge.tags, "lineage_edge attribute 'tags' is None")
        self.assertEqual(lineage_edge.tags, {'testKey': 'testValue'},
                         "lineage_edge attribute 'sourceKey', Expected: "
                         "" + str({'testKey': 'testValue'}) + ", Actual: " + str(lineage_edge.tags))
        lineage_edge_json = lineage_edge.to_json()
        self.assertEqual(lineage_edge_json, '{"class": "LineageEdge", "tags": {"testKey": "testValue"}, '
                                            '"sourceKey": "testSourceKey", "lineageEdgeId": 0, "name": "testName"}')

    def test_git_get_lineage_graph(self):
        git = ground.GitImplementation()
        lineageGraphId = git.createLineageGraph('testSourceKey', 'testName', {'testKey': 'testValue'})
        lineage_graph = git.getLineageGraph('testSourceKey')
        self.assertIsNotNone(lineage_graph.sourceKey, "lineage_graph attribute 'sourceKey' is None")
        self.assertEqual(lineage_graph.sourceKey, 'testSourceKey', "lineage_graph attribute 'sourceKey', "
                                                                   "Expected: testSourceKey, "
                                                                   "Actual: " + str(lineage_graph.sourceKey))
        self.assertIsNotNone(lineage_graph.name, "lineage_graph attribute 'name' is None")
        self.assertEqual(lineage_graph.name, 'testName', "lineage_graph attribute 'name', Expected: testName, "
                                                         "Actual: " + str(lineage_graph.name))
        self.assertIsNotNone(lineage_graph.tags, "lineage_graph attribute 'tags' is None")
        self.assertEqual(lineage_graph.tags, {'testKey': 'testValue'},
                         "lineage_graph attribute 'sourceKey', Expected: "
                         "" + str({'testKey': 'testValue'}) + ", Actual: " + str(lineage_graph.tags))
        lineage_graph_json = lineage_graph.to_json()
        self.assertEqual(lineage_graph_json, '{"lineageGraphId": 0, "class": "LineageGraph", '
                                             '"tags": {"testKey": "testValue"}, '
                                             '"sourceKey": "testSourceKey", "name": "testName"}')

    def test_git_create_node_version(self):
        git = ground.GitImplementation()
        nodeId = git.createNode('testSourceKey', 'testName', {'testKey': 'testValue'})
        nodeVersionId = git.createNodeVersion(nodeId, "testReference", "testReferenceParameters",
                                              {'testKey': 'testValue'}, 1, [2, 3])
        node_version = git.graph.nodeVersions[nodeVersionId]
        self.assertIsNotNone(node_version.sourceKey, "node_version attribute 'sourceKey' is None")
        self.assertEqual(node_version.sourceKey, 'testSourceKey', "node_version attribute 'sourceKey', "
                                                                  "Expected: testSourceKey, "
                                                                  "Actual: " + str(node_version.sourceKey))
        self.assertIsNotNone(node_version.nodeId, "node_version attribute 'nodeId' is None")
        self.assertEqual(node_version.nodeId, 0, "node_version attribute 'nodeId', "
                                                 "Expected: 0, Actual: " + str(node_version.nodeId))
        self.assertIsNotNone(node_version.reference, "node_version attribute 'reference' is None")
        self.assertEqual(node_version.reference, "testReference", "node_version attribute 'reference', "
                                                                  "Expected: testReference, "
                                                                  "Actual: " + str(node_version.reference))
        self.assertIsNotNone(node_version.referenceParameters, "node_version attribute 'referenceParameters' is None")
        self.assertEqual(node_version.referenceParameters, "testReferenceParameters", "node_version attribute "
                                                                                      "'referenceParameters', "
                                                                                      "Expected: testReferenceParameters, "
                                                                                      "Actual: " + str(
            node_version.referenceParameters))
        self.assertIsNotNone(node_version.tags, "node_version attribute 'tags' is None")
        self.assertEqual(node_version.tags,
                         {'testKey': 'testValue'},
                         "node_version attribute 'tags', "
                         "Expected: " + str({'testKey': 'testValue'}) + ", Actual: " + str(node_version.tags))
        self.assertIsNotNone(node_version.structureVersionId, "node_version attribute 'structureVersionId' is None")
        self.assertEqual(node_version.structureVersionId, 1, "node_version attribute 'structureVersionId', "
                                                             "Expected: 1, "
                                                             "Actual: " + str(node_version.structureVersionId))
        self.assertIsNotNone(node_version.parentIds, "node_version attribute 'parentIds' is None")
        self.assertEqual(node_version.parentIds, [2, 3], "node_version attribute 'parentIds', "
                                                         "Expected: [2, 3], "
                                                         "Actual: " + str(node_version.parentIds))
        node_version_json = node_version.to_json()
        self.assertEqual(node_version_json, '{"nodeVersionId": 1, "reference": "testReference", '
                                            '"tags": {"testKey": "testValue"}, '
                                            '"referenceParameters": "testReferenceParameters", "class": "NodeVersion", '
                                            '"parentIds": [2, 3], "structureVersionId": 1, '
                                            '"sourceKey": "testSourceKey", "nodeId": 0}')

    def test_git_create_edge_version(self):
        git = ground.GitImplementation()
        edgeId = git.createEdge('testSourceKey', 0, 10, 'testName', {'testKey': 'testValue'})
        edgeVersionId = git.createEdgeVersion(edgeId, 4, 5, 6, 7, "testReference", "testReferenceParameters",
                                              {'testKey': 'testValue'}, 1, [2, 3])
        edge_version = git.graph.edgeVersions[edgeVersionId]
        self.assertIsNotNone(edge_version.sourceKey, "edge_version attribute 'sourceKey' is None")
        self.assertEqual(edge_version.sourceKey, 'testSourceKey', "edge_version attribute 'sourceKey', "
                                                                  "Expected: testSourceKey, "
                                                                  "Actual: " + str(edge_version.sourceKey))
        self.assertIsNotNone(edge_version.fromNodeId, "edge_version attribute 'fromNodeId' is None")
        self.assertEqual(edge_version.fromNodeId, 0, "edge_version attribute 'fromNodeId', "
                                                     "Expected: 0, Actual: " + str(edge_version.fromNodeId))
        self.assertIsNotNone(edge_version.toNodeId, "edge_version attribute 'toNodeId' is None")
        self.assertEqual(edge_version.toNodeId, 10, "edge_version attribute 'toNodeId', "
                                                    "Expected: 10, Actual: " + str(edge_version.toNodeId))
        self.assertIsNotNone(edge_version.edgeId, "edge_version attribute 'edgeId' is None")
        self.assertEqual(edge_version.edgeId, 0, "edge_version attribute 'edgeId', "
                                                 "Expected: 0, Actual: " + str(edge_version.edgeId))
        self.assertIsNotNone(edge_version.toNodeVersionStartId, "edge_version attribute 'toNodeVersionStartId' is None")
        self.assertEqual(edge_version.toNodeVersionStartId, 4, "edge_version attribute 'toNodeVersionStartId', "
                                                               "Expected: 4, Actual: " + str(
            edge_version.toNodeVersionStartId))
        self.assertIsNotNone(edge_version.fromNodeVersionStartId, "edge_version attribute 'fromNodeVersionStartId' "
                                                                  "is None")
        self.assertEqual(edge_version.fromNodeVersionStartId, 5, "edge_version attribute 'fromNodeVersionStartId', "
                                                                 "Expected: 5, Actual: " + str(
            edge_version.fromNodeVersionStartId))
        self.assertIsNotNone(edge_version.toNodeVersionEndId, "edge_version attribute 'toNodeVersionEndId' is None")
        self.assertEqual(edge_version.toNodeVersionEndId, 6, "edge_version attribute 'toNodeVersionEndId', "
                                                             "Expected: 6, Actual: " + str(
            edge_version.toNodeVersionEndId))
        self.assertIsNotNone(edge_version.fromNodeVersionEndId, "edge_version attribute 'fromNodeVersionEndId' is None")
        self.assertEqual(edge_version.fromNodeVersionEndId, 7, "edge_version attribute 'fromNodeVersionEndId', "
                                                               "Expected: 7, Actual: " + str(
            edge_version.fromNodeVersionEndId))
        self.assertIsNotNone(edge_version.reference, "edge_version attribute 'reference' is None")
        self.assertEqual(edge_version.reference, "testReference", "edge_version attribute 'reference', "
                                                                  "Expected: testReference, "
                                                                  "Actual: " + str(edge_version.reference))
        self.assertIsNotNone(edge_version.referenceParameters, "edge_version attribute 'referenceParameters' is None")
        self.assertEqual(edge_version.referenceParameters, "testReferenceParameters", "edge_version attribute "
                                                                                      "'referenceParameters', "
                                                                                      "Expected: testReferenceParameters, "
                                                                                      "Actual: " + str(
            edge_version.referenceParameters))
        self.assertIsNotNone(edge_version.tags, "edge_version attribute 'tags' is None")
        self.assertEqual(edge_version.tags,
                         {'testKey': 'testValue'},
                         "edge_version attribute 'tags', "
                         "Expected: " + str({'testKey': 'testValue'}) + ", Actual: " + str(edge_version.tags))
        self.assertIsNotNone(edge_version.structureVersionId, "edge_version attribute 'structureVersionId' is None")
        self.assertEqual(edge_version.structureVersionId, 1, "edge_version attribute 'structureVersionId', "
                                                             "Expected: 1, "
                                                             "Actual: " + str(edge_version.structureVersionId))
        self.assertIsNotNone(edge_version.parentIds, "edge_version attribute 'parentIds' is None")
        self.assertEqual(edge_version.parentIds, [2, 3], "edge_version attribute 'parentIds', "
                                                         "Expected: [2, 3], "
                                                         "Actual: " + str(edge_version.parentIds))
        edge_version_json = edge_version.to_json()
        self.assertEqual(edge_version_json, '{"toNodeVersionStartId": 4, "toNodeVersionEndId": 6, '
                                            '"reference": "testReference", "tags": {"testKey": "testValue"}, '
                                            '"edgeVersionId": 1, "referenceParameters": "testReferenceParameters", '
                                            '"class": "EdgeVersion", "fromNodeId": 0, "edgeId": 0, '
                                            '"parentIds": [2, 3], "structureVersionId": 1, '
                                            '"fromNodeVersionStartId": 5, "toNodeId": 10, '
                                            '"fromNodeVersionEndId": 7, "sourceKey": "testSourceKey"}')

    def test_git_create_graph_version(self):
        git = ground.GitImplementation()
        graphId = git.createGraph('testSourceKey', 'testName', {'testKey': 'testValue'})
        graphVersionId = git.createGraphVersion(graphId, [4, 5, 6], "testReference", "testReferenceParameters",
                                                {'testKey': 'testValue'}, 1, [2, 3])
        graph_version = git.graph.graphVersions[graphVersionId]
        self.assertIsNotNone(graph_version.sourceKey, "graph_version attribute 'sourceKey' is None")
        self.assertEqual(graph_version.sourceKey, 'testSourceKey', "graph_version attribute 'sourceKey', "
                                                                   "Expected: testSourceKey, "
                                                                   "Actual: " + str(graph_version.sourceKey))
        self.assertIsNotNone(graph_version.graphId, "graph_version attribute 'graphId' is None")
        self.assertEqual(graph_version.graphId, 0, "graph_version attribute 'graphId', "
                                                   "Expected: 0, Actual: " + str(graph_version.graphId))
        self.assertIsNotNone(graph_version.edgeVersionIds, "graph_version attribute 'edgeVersionIds' is None")
        self.assertEqual(graph_version.edgeVersionIds, [4, 5, 6], "graph_version attribute 'edgeVersionIds', "
                                                                  "Expected: [4, 5, 6], "
                                                                  "Actual: " + str(graph_version.edgeVersionIds))
        self.assertIsNotNone(graph_version.reference, "graph_version attribute 'reference' is None")
        self.assertEqual(graph_version.reference, "testReference", "graph_version attribute 'reference', "
                                                                   "Expected: testReference, "
                                                                   "Actual: " + str(graph_version.reference))
        self.assertIsNotNone(graph_version.referenceParameters, "graph_version attribute 'referenceParameters' is None")
        self.assertEqual(graph_version.referenceParameters, "testReferenceParameters", "graph_version attribute "
                                                                                       "'referenceParameters', "
                                                                                       "Expected: testReferenceParameters, "
                                                                                       "Actual: " + str(
            graph_version.referenceParameters))
        self.assertIsNotNone(graph_version.tags, "graph_version attribute 'tags' is None")
        self.assertEqual(graph_version.tags,
                         {'testKey': 'testValue'},
                         "graph_version attribute 'tags', "
                         "Expected: " + str({'testKey': 'testValue'}) + ", Actual: " + str(graph_version.tags))
        self.assertIsNotNone(graph_version.structureVersionId, "graph_version attribute 'structureVersionId' is None")
        self.assertEqual(graph_version.structureVersionId, 1, "graph_version attribute 'structureVersionId', "
                                                              "Expected: 1, "
                                                              "Actual: " + str(graph_version.structureVersionId))
        self.assertIsNotNone(graph_version.parentIds, "graph_version attribute 'parentIds' is None")
        self.assertEqual(graph_version.parentIds, [2, 3], "graph_version attribute 'parentIds', "
                                                          "Expected: [2, 3], "
                                                          "Actual: " + str(graph_version.parentIds))
        graph_version_json = graph_version.to_json()
        self.assertEqual(graph_version_json, '{"parentIds": [2, 3], "graphId": 0, "reference": "testReference", '
                                             '"edgeVersionIds": [4, 5, 6], '
                                             '"referenceParameters": "testReferenceParameters", '
                                             '"graphVersionId": 1, "tags": {"testKey": "testValue"}, '
                                             '"structureVersionId": 1, "sourceKey": "testSourceKey", '
                                             '"class": "GraphVersion"}')

    def test_git_create_structure_version(self):
        git = ground.GitImplementation()
        structureId = git.createStructure('testSourceKey', 'testName', {'testKey': 'testValue'})
        structureVersionId = git.createStructureVersion(structureId, {'testKey': 'testValue'}, [2, 3])
        structure_version = git.graph.structureVersions[structureVersionId]
        self.assertIsNotNone(structure_version.sourceKey, "structure_version attribute 'sourceKey' is None")
        self.assertEqual(structure_version.sourceKey, 'testSourceKey', "structure_version attribute 'sourceKey', "
                                                                       "Expected: testSourceKey, "
                                                                       "Actual: " + str(structure_version.sourceKey))
        self.assertIsNotNone(structure_version.structureId, "structure_version attribute 'structureId' is None")
        self.assertEqual(structure_version.structureId, 0, "structure_version attribute 'structureId', "
                                                           "Expected: 0, Actual: " + str(structure_version.structureId))
        self.assertIsNotNone(structure_version.attributes, "structure_version attribute 'attributes' is None")
        self.assertEqual(structure_version.attributes, {'testKey': 'testValue'}, "structure_version "
                                                                                 "attribute 'attributes', "
                                                                                 "Expected: , " + str(
            {'testKey': 'testValue'}) +
                         ", Actual: " + str(structure_version.attributes))
        self.assertIsNotNone(structure_version.parentIds, "structure_version attribute 'parentIds' is None")
        self.assertEqual(structure_version.parentIds, [2, 3], "structure_version attribute 'parentIds', "
                                                              "Expected: [2, 3], "
                                                              "Actual: " + str(structure_version.parentIds))
        structure_version_json = structure_version.to_json()
        self.assertEqual(structure_version_json, '{"parentIds": [2, 3], "structureId": 0, "structureVersionId": 1, '
                                                 '"sourceKey": "testSourceKey", '
                                                 '"attributes": {"testKey": "testValue"}, '
                                                 '"class": "StructureVersion"}')

    def test_git_create_lineage_edge_version(self):
        git = ground.GitImplementation()
        lineageEdgeId = git.createLineageEdge('testSourceKey', 'testName', {'testKey': 'testValue'})
        lineageEdgeVersionId = git.createLineageEdgeVersion(lineageEdgeId, 5, 4, "testReference",
                                                            "testReferenceParameters",
                                                            {'testKey': 'testValue'}, 1, [2, 3])
        lineage_edge_version = git.graph.lineageEdgeVersions[lineageEdgeVersionId]
        self.assertIsNotNone(lineage_edge_version.sourceKey, "lineage_edge_version attribute 'sourceKey' is None")
        self.assertEqual(lineage_edge_version.sourceKey, 'testSourceKey', "lineage_edge_version attribute 'sourceKey', "
                                                                          "Expected: testSourceKey, "
                                                                          "Actual: " + str(
            lineage_edge_version.sourceKey))
        self.assertIsNotNone(lineage_edge_version.lineageEdgeId, "lineage_edge_version attribute "
                                                                 "'lineageEdgeId' is None")
        self.assertEqual(lineage_edge_version.lineageEdgeId, 0, "lineage_edge_version attribute 'lineageEdgeId', "
                                                                "Expected: 0, Actual: " + str(
            lineage_edge_version.lineageEdgeId))
        self.assertIsNotNone(lineage_edge_version.fromRichVersionId, "lineage_edge_version attribute "
                                                                     "'fromRichVersionId' is None")
        self.assertEqual(lineage_edge_version.fromRichVersionId, 4,
                         "lineage_edge_version attribute 'fromRichVersionId', "
                         "Expected: 4, Actual: " + str(lineage_edge_version.fromRichVersionId))
        self.assertIsNotNone(lineage_edge_version.toRichVersionId, "lineage_edge_version attribute "
                                                                   "'toRichVersionId' is None")
        self.assertEqual(lineage_edge_version.toRichVersionId, 5, "lineage_edge_version attribute 'toRichVersionId', "
                                                                  "Expected: 5, Actual: " + str(
            lineage_edge_version.toRichVersionId))
        self.assertIsNotNone(lineage_edge_version.reference, "lineage_edge_version attribute 'reference' is None")
        self.assertEqual(lineage_edge_version.reference, "testReference", "lineage_edge_version attribute 'reference', "
                                                                          "Expected: testReference, "
                                                                          "Actual: " + str(
            lineage_edge_version.reference))
        self.assertIsNotNone(lineage_edge_version.referenceParameters, "lineage_edge_version attribute "
                                                                       "'referenceParameters' is None")
        self.assertEqual(lineage_edge_version.referenceParameters, "testReferenceParameters",
                         "lineage_edge_version "
                         "attribute "
                         "'referenceParameters', "
                         "Expected: testReferenceParameters, "
                         "Actual: " + str(lineage_edge_version.referenceParameters))
        self.assertIsNotNone(lineage_edge_version.tags, "lineage_edge_version attribute 'tags' is None")
        self.assertEqual(lineage_edge_version.tags,
                         {'testKey': 'testValue'},
                         "lineage_edge_version attribute 'tags', "
                         "Expected: " + str({'testKey': 'testValue'}) + ", Actual: " + str(lineage_edge_version.tags))
        self.assertIsNotNone(lineage_edge_version.structureVersionId,
                             "lineage_edge_version attribute 'structureVersionId' is None")
        self.assertEqual(lineage_edge_version.structureVersionId, 1,
                         "lineage_edge_version attribute 'structureVersionId', "
                         "Expected: 1, "
                         "Actual: " + str(lineage_edge_version.structureVersionId))
        self.assertIsNotNone(lineage_edge_version.parentIds, "lineage_edge_version attribute 'parentIds' is None")
        self.assertEqual(lineage_edge_version.parentIds, [2, 3], "lineage_edge_version attribute 'parentIds', "
                                                                 "Expected: [2, 3], "
                                                                 "Actual: " + str(lineage_edge_version.parentIds))
        lineage_edge_version_json = lineage_edge_version.to_json()
        self.assertEqual(lineage_edge_version_json, '{"reference": "testReference", "tags": {"testKey": "testValue"}, '
                                                    '"lineageEdgeVersionId": 1, '
                                                    '"referenceParameters": "testReferenceParameters", '
                                                    '"fromRichVersionId": 4, "class": "LineageEdgeVersion", '
                                                    '"parentIds": [2, 3], "structureVersionId": 1, '
                                                    '"toRichVersionId": 5, "lineageEdgeId": 0, '
                                                    '"sourceKey": "testSourceKey"}')

    def test_git_create_lineage_graph_version(self):
        git = ground.GitImplementation()
        lineageGraphId = git.createLineageGraph('testSourceKey', 'testName', {'testKey': 'testValue'})
        lineageGraphVersionId = git.createLineageGraphVersion(lineageGraphId, [5, 4], "testReference",
                                                              "testReferenceParameters",
                                                              {'testKey': 'testValue'}, 1, [2, 3])
        lineage_graph_version = git.graph.lineageGraphVersions[lineageGraphVersionId]
        self.assertIsNotNone(lineage_graph_version.sourceKey, "lineage_graph_version attribute 'sourceKey' is None")
        self.assertEqual(lineage_graph_version.sourceKey, 'testSourceKey',
                         "lineage_graph_version attribute 'sourceKey', "
                         "Expected: testSourceKey, "
                         "Actual: " + str(
                             lineage_graph_version.sourceKey))
        self.assertIsNotNone(lineage_graph_version.lineageGraphId, "lineage_graph_version attribute "
                                                                   "'lineageGraphId' is None")
        self.assertEqual(lineage_graph_version.lineageGraphId, 0, "lineage_graph_version attribute 'lineageGraphId', "
                                                                  "Expected: 0, Actual: " + str(
            lineage_graph_version.lineageGraphId))
        self.assertIsNotNone(lineage_graph_version.lineageEdgeVersionIds, "lineage_graph_version attribute "
                                                                          "'lineageEdgeVersionIds' is None")
        self.assertEqual(lineage_graph_version.lineageEdgeVersionIds, [5, 4],
                         "lineage_graph_version attribute 'lineageEdgeVersionIds', "
                         "Expected: [5, 4], Actual: " + str(lineage_graph_version.lineageEdgeVersionIds))
        self.assertIsNotNone(lineage_graph_version.reference, "lineage_graph_version attribute 'reference' is None")
        self.assertEqual(lineage_graph_version.reference, "testReference",
                         "lineage_graph_version attribute 'reference', "
                         "Expected: testReference, "
                         "Actual: " + str(
                             lineage_graph_version.reference))
        self.assertIsNotNone(lineage_graph_version.referenceParameters, "lineage_graph_version attribute "
                                                                        "'referenceParameters' is None")
        self.assertEqual(lineage_graph_version.referenceParameters, "testReferenceParameters",
                         "lineage_graph_version "
                         "attribute "
                         "'referenceParameters', "
                         "Expected: testReferenceParameters, "
                         "Actual: " + str(lineage_graph_version.referenceParameters))
        self.assertIsNotNone(lineage_graph_version.tags, "lineage_graph_version attribute 'tags' is None")
        self.assertEqual(lineage_graph_version.tags,
                         {'testKey': 'testValue'},
                         "lineage_graph_version attribute 'tags', "
                         "Expected: " + str({'testKey': 'testValue'}) + ", Actual: " + str(lineage_graph_version.tags))
        self.assertIsNotNone(lineage_graph_version.structureVersionId,
                             "lineage_graph_version attribute 'structureVersionId' is None")
        self.assertEqual(lineage_graph_version.structureVersionId, 1,
                         "lineage_graph_version attribute 'structureVersionId', "
                         "Expected: 1, "
                         "Actual: " + str(lineage_graph_version.structureVersionId))
        self.assertIsNotNone(lineage_graph_version.parentIds, "lineage_graph_version attribute 'parentIds' is None")
        self.assertEqual(lineage_graph_version.parentIds, [2, 3], "lineage_graph_version attribute 'parentIds', "
                                                                  "Expected: [2, 3], "
                                                                  "Actual: " + str(lineage_graph_version.parentIds))
        lineage_graph_version_json = lineage_graph_version.to_json()
        self.assertEqual(lineage_graph_version_json, '{"lineageGraphId": 0, "lineageGraphVersionId": 1, '
                                                     '"parentIds": [2, 3], "reference": "testReference", '
                                                     '"tags": {"testKey": "testValue"}, '
                                                     '"referenceParameters": "testReferenceParameters", '
                                                     '"lineageEdgeVersionIds": [5, 4], '
                                                     '"structureVersionId": 1, "sourceKey": "testSourceKey", '
                                                     '"class": "LineageGraphVersion"}')

    def test_git_get_node_version(self):
        git = ground.GitImplementation()
        nodeId = git.createNode('testSourceKey', 'testName', {'testKey': 'testValue'})
        nodeVersionId = git.createNodeVersion(nodeId, "testReference", "testReferenceParameters",
                                              {'testKey': 'testValue'}, 1, [2, 3])
        node_version = git.getNodeVersion(nodeVersionId)
        self.assertIsNotNone(node_version.sourceKey, "node_version attribute 'sourceKey' is None")
        self.assertEqual(node_version.sourceKey, 'testSourceKey', "node_version attribute 'sourceKey', "
                                                                  "Expected: testSourceKey, "
                                                                  "Actual: " + str(node_version.sourceKey))
        self.assertIsNotNone(node_version.nodeId, "node_version attribute 'nodeId' is None")
        self.assertEqual(node_version.nodeId, 0, "node_version attribute 'nodeId', "
                                                 "Expected: 0, Actual: " + str(node_version.nodeId))
        self.assertIsNotNone(node_version.reference, "node_version attribute 'reference' is None")
        self.assertEqual(node_version.reference, "testReference", "node_version attribute 'reference', "
                                                                  "Expected: testReference, "
                                                                  "Actual: " + str(node_version.reference))
        self.assertIsNotNone(node_version.referenceParameters, "node_version attribute 'referenceParameters' is None")
        self.assertEqual(node_version.referenceParameters, "testReferenceParameters", "node_version attribute "
                                                                                      "'referenceParameters', "
                                                                                      "Expected: testReferenceParameters, "
                                                                                      "Actual: " + str(
            node_version.referenceParameters))
        self.assertIsNotNone(node_version.tags, "node_version attribute 'tags' is None")
        self.assertEqual(node_version.tags,
                         {'testKey': 'testValue'},
                         "node_version attribute 'tags', "
                         "Expected: " + str({'testKey': 'testValue'}) + ", Actual: " + str(node_version.tags))
        self.assertIsNotNone(node_version.structureVersionId, "node_version attribute 'structureVersionId' is None")
        self.assertEqual(node_version.structureVersionId, 1, "node_version attribute 'structureVersionId', "
                                                             "Expected: 1, "
                                                             "Actual: " + str(node_version.structureVersionId))
        self.assertIsNotNone(node_version.parentIds, "node_version attribute 'parentIds' is None")
        self.assertEqual(node_version.parentIds, [2, 3], "node_version attribute 'parentIds', "
                                                         "Expected: [2, 3], "
                                                         "Actual: " + str(node_version.parentIds))
        node_version_json = node_version.to_json()
        self.assertEqual(node_version_json, '{"nodeVersionId": 1, "reference": "testReference", '
                                            '"tags": {"testKey": "testValue"}, '
                                            '"referenceParameters": "testReferenceParameters", "class": "NodeVersion", '
                                            '"parentIds": [2, 3], "structureVersionId": 1, '
                                            '"sourceKey": "testSourceKey", "nodeId": 0}')

    def test_git_get_edge_version(self):
        git = ground.GitImplementation()
        edgeId = git.createEdge('testSourceKey', 0, 10, 'testName', {'testKey': 'testValue'})
        edgeVersionId = git.createEdgeVersion(edgeId, 4, 5, 6, 7, "testReference", "testReferenceParameters",
                                              {'testKey': 'testValue'}, 1, [2, 3])
        edge_version = git.getEdgeVersion(edgeVersionId)
        self.assertIsNotNone(edge_version.sourceKey, "edge_version attribute 'sourceKey' is None")
        self.assertEqual(edge_version.sourceKey, 'testSourceKey', "edge_version attribute 'sourceKey', "
                                                                  "Expected: testSourceKey, "
                                                                  "Actual: " + str(edge_version.sourceKey))
        self.assertIsNotNone(edge_version.fromNodeId, "edge_version attribute 'fromNodeId' is None")
        self.assertEqual(edge_version.fromNodeId, 0, "edge_version attribute 'fromNodeId', "
                                                     "Expected: 0, Actual: " + str(edge_version.fromNodeId))
        self.assertIsNotNone(edge_version.toNodeId, "edge_version attribute 'toNodeId' is None")
        self.assertEqual(edge_version.toNodeId, 10, "edge_version attribute 'toNodeId', "
                                                    "Expected: 10, Actual: " + str(edge_version.toNodeId))
        self.assertIsNotNone(edge_version.edgeId, "edge_version attribute 'edgeId' is None")
        self.assertEqual(edge_version.edgeId, 0, "edge_version attribute 'edgeId', "
                                                 "Expected: 0, Actual: " + str(edge_version.edgeId))
        self.assertIsNotNone(edge_version.toNodeVersionStartId, "edge_version attribute 'toNodeVersionStartId' is None")
        self.assertEqual(edge_version.toNodeVersionStartId, 4, "edge_version attribute 'toNodeVersionStartId', "
                                                               "Expected: 4, Actual: " + str(
            edge_version.toNodeVersionStartId))
        self.assertIsNotNone(edge_version.fromNodeVersionStartId, "edge_version attribute 'fromNodeVersionStartId' "
                                                                  "is None")
        self.assertEqual(edge_version.fromNodeVersionStartId, 5, "edge_version attribute 'fromNodeVersionStartId', "
                                                                 "Expected: 5, Actual: " + str(
            edge_version.fromNodeVersionStartId))
        self.assertIsNotNone(edge_version.toNodeVersionEndId, "edge_version attribute 'toNodeVersionEndId' is None")
        self.assertEqual(edge_version.toNodeVersionEndId, 6, "edge_version attribute 'toNodeVersionEndId', "
                                                             "Expected: 6, Actual: " + str(
            edge_version.toNodeVersionEndId))
        self.assertIsNotNone(edge_version.fromNodeVersionEndId, "edge_version attribute 'fromNodeVersionEndId' is None")
        self.assertEqual(edge_version.fromNodeVersionEndId, 7, "edge_version attribute 'fromNodeVersionEndId', "
                                                               "Expected: 7, Actual: " + str(
            edge_version.fromNodeVersionEndId))
        self.assertIsNotNone(edge_version.reference, "edge_version attribute 'reference' is None")
        self.assertEqual(edge_version.reference, "testReference", "edge_version attribute 'reference', "
                                                                  "Expected: testReference, "
                                                                  "Actual: " + str(edge_version.reference))
        self.assertIsNotNone(edge_version.referenceParameters, "edge_version attribute 'referenceParameters' is None")
        self.assertEqual(edge_version.referenceParameters, "testReferenceParameters", "edge_version attribute "
                                                                                      "'referenceParameters', "
                                                                                      "Expected: testReferenceParameters, "
                                                                                      "Actual: " + str(
            edge_version.referenceParameters))
        self.assertIsNotNone(edge_version.tags, "edge_version attribute 'tags' is None")
        self.assertEqual(edge_version.tags,
                         {'testKey': 'testValue'},
                         "edge_version attribute 'tags', "
                         "Expected: " + str({'testKey': 'testValue'}) + ", Actual: " + str(edge_version.tags))
        self.assertIsNotNone(edge_version.structureVersionId, "edge_version attribute 'structureVersionId' is None")
        self.assertEqual(edge_version.structureVersionId, 1, "edge_version attribute 'structureVersionId', "
                                                             "Expected: 1, "
                                                             "Actual: " + str(edge_version.structureVersionId))
        self.assertIsNotNone(edge_version.parentIds, "edge_version attribute 'parentIds' is None")
        self.assertEqual(edge_version.parentIds, [2, 3], "edge_version attribute 'parentIds', "
                                                         "Expected: [2, 3], "
                                                         "Actual: " + str(edge_version.parentIds))
        edge_version_json = edge_version.to_json()
        self.assertEqual(edge_version_json, '{"toNodeVersionStartId": 4, "toNodeVersionEndId": 6, '
                                            '"reference": "testReference", "tags": {"testKey": "testValue"}, '
                                            '"edgeVersionId": 1, "referenceParameters": "testReferenceParameters", '
                                            '"class": "EdgeVersion", "fromNodeId": 0, "edgeId": 0, '
                                            '"parentIds": [2, 3], "structureVersionId": 1, '
                                            '"fromNodeVersionStartId": 5, "toNodeId": 10, '
                                            '"fromNodeVersionEndId": 7, "sourceKey": "testSourceKey"}')

    def test_git_get_graph_version(self):
        git = ground.GitImplementation()
        graphId = git.createGraph('testSourceKey', 'testName', {'testKey': 'testValue'})
        graphVersionId = git.createGraphVersion(graphId, [4, 5, 6], "testReference", "testReferenceParameters",
                                                {'testKey': 'testValue'}, 1, [2, 3])
        graph_version = git.getGraphVersion(graphVersionId)
        self.assertIsNotNone(graph_version.sourceKey, "graph_version attribute 'sourceKey' is None")
        self.assertEqual(graph_version.sourceKey, 'testSourceKey', "graph_version attribute 'sourceKey', "
                                                                   "Expected: testSourceKey, "
                                                                   "Actual: " + str(graph_version.sourceKey))
        self.assertIsNotNone(graph_version.graphId, "graph_version attribute 'graphId' is None")
        self.assertEqual(graph_version.graphId, 0, "graph_version attribute 'graphId', "
                                                   "Expected: 0, Actual: " + str(graph_version.graphId))
        self.assertIsNotNone(graph_version.edgeVersionIds, "graph_version attribute 'edgeVersionIds' is None")
        self.assertEqual(graph_version.edgeVersionIds, [4, 5, 6], "graph_version attribute 'edgeVersionIds', "
                                                                  "Expected: [4, 5, 6], "
                                                                  "Actual: " + str(graph_version.edgeVersionIds))
        self.assertIsNotNone(graph_version.reference, "graph_version attribute 'reference' is None")
        self.assertEqual(graph_version.reference, "testReference", "graph_version attribute 'reference', "
                                                                   "Expected: testReference, "
                                                                   "Actual: " + str(graph_version.reference))
        self.assertIsNotNone(graph_version.referenceParameters, "graph_version attribute 'referenceParameters' is None")
        self.assertEqual(graph_version.referenceParameters, "testReferenceParameters", "graph_version attribute "
                                                                                       "'referenceParameters', "
                                                                                       "Expected: testReferenceParameters, "
                                                                                       "Actual: " + str(
            graph_version.referenceParameters))
        self.assertIsNotNone(graph_version.tags, "graph_version attribute 'tags' is None")
        self.assertEqual(graph_version.tags,
                         {'testKey': 'testValue'},
                         "graph_version attribute 'tags', "
                         "Expected: " + str({'testKey': 'testValue'}) + ", Actual: " + str(graph_version.tags))
        self.assertIsNotNone(graph_version.structureVersionId, "graph_version attribute 'structureVersionId' is None")
        self.assertEqual(graph_version.structureVersionId, 1, "graph_version attribute 'structureVersionId', "
                                                              "Expected: 1, "
                                                              "Actual: " + str(graph_version.structureVersionId))
        self.assertIsNotNone(graph_version.parentIds, "graph_version attribute 'parentIds' is None")
        self.assertEqual(graph_version.parentIds, [2, 3], "graph_version attribute 'parentIds', "
                                                          "Expected: [2, 3], "
                                                          "Actual: " + str(graph_version.parentIds))
        graph_version_json = graph_version.to_json()
        self.assertEqual(graph_version_json, '{"parentIds": [2, 3], "graphId": 0, "reference": "testReference", '
                                             '"edgeVersionIds": [4, 5, 6], '
                                             '"referenceParameters": "testReferenceParameters", '
                                             '"graphVersionId": 1, "tags": {"testKey": "testValue"}, '
                                             '"structureVersionId": 1, "sourceKey": "testSourceKey", '
                                             '"class": "GraphVersion"}')

    def test_git_get_structure_version(self):
        git = ground.GitImplementation()
        structureId = git.createStructure('testSourceKey', 'testName', {'testKey': 'testValue'})
        structureVersionId = git.createStructureVersion(structureId, {'testKey': 'testValue'}, [2, 3])
        structure_version = git.getStructureVersion(structureVersionId)
        self.assertIsNotNone(structure_version.sourceKey, "structure_version attribute 'sourceKey' is None")
        self.assertEqual(structure_version.sourceKey, 'testSourceKey', "structure_version attribute 'sourceKey', "
                                                                       "Expected: testSourceKey, "
                                                                       "Actual: " + str(structure_version.sourceKey))
        self.assertIsNotNone(structure_version.structureId, "structure_version attribute 'structureId' is None")
        self.assertEqual(structure_version.structureId, 0, "structure_version attribute 'structureId', "
                                                           "Expected: 0, Actual: " + str(structure_version.structureId))
        self.assertIsNotNone(structure_version.attributes, "structure_version attribute 'attributes' is None")
        self.assertEqual(structure_version.attributes, {'testKey': 'testValue'}, "structure_version "
                                                                                 "attribute 'attributes', "
                                                                                 "Expected: , " + str(
            {'testKey': 'testValue'}) +
                         ", Actual: " + str(structure_version.attributes))
        self.assertIsNotNone(structure_version.parentIds, "structure_version attribute 'parentIds' is None")
        self.assertEqual(structure_version.parentIds, [2, 3], "structure_version attribute 'parentIds', "
                                                              "Expected: [2, 3], "
                                                              "Actual: " + str(structure_version.parentIds))
        structure_version_json = structure_version.to_json()
        self.assertEqual(structure_version_json, '{"parentIds": [2, 3], "structureId": 0, "structureVersionId": 1, '
                                                 '"sourceKey": "testSourceKey", '
                                                 '"attributes": {"testKey": "testValue"}, '
                                                 '"class": "StructureVersion"}')

    def test_git_get_lineage_edge_version(self):
        git = ground.GitImplementation()
        lineageEdgeId = git.createLineageEdge('testSourceKey', 'testName', {'testKey': 'testValue'})
        lineageEdgeVersionId = git.createLineageEdgeVersion(lineageEdgeId, 5, 4, "testReference",
                                                            "testReferenceParameters",
                                                            {'testKey': 'testValue'}, 1, [2, 3])
        lineage_edge_version = git.getLineageEdgeVersion(lineageEdgeVersionId)
        self.assertIsNotNone(lineage_edge_version.sourceKey, "lineage_edge_version attribute 'sourceKey' is None")
        self.assertEqual(lineage_edge_version.sourceKey, 'testSourceKey', "lineage_edge_version attribute 'sourceKey', "
                                                                          "Expected: testSourceKey, "
                                                                          "Actual: " + str(
            lineage_edge_version.sourceKey))
        self.assertIsNotNone(lineage_edge_version.lineageEdgeId, "lineage_edge_version attribute "
                                                                 "'lineageEdgeId' is None")
        self.assertEqual(lineage_edge_version.lineageEdgeId, 0, "lineage_edge_version attribute 'lineageEdgeId', "
                                                                "Expected: 0, Actual: " + str(
            lineage_edge_version.lineageEdgeId))
        self.assertIsNotNone(lineage_edge_version.fromRichVersionId, "lineage_edge_version attribute "
                                                                     "'fromRichVersionId' is None")
        self.assertEqual(lineage_edge_version.fromRichVersionId, 4,
                         "lineage_edge_version attribute 'fromRichVersionId', "
                         "Expected: 4, Actual: " + str(lineage_edge_version.fromRichVersionId))
        self.assertIsNotNone(lineage_edge_version.toRichVersionId, "lineage_edge_version attribute "
                                                                   "'toRichVersionId' is None")
        self.assertEqual(lineage_edge_version.toRichVersionId, 5, "lineage_edge_version attribute 'toRichVersionId', "
                                                                  "Expected: 5, Actual: " + str(
            lineage_edge_version.toRichVersionId))
        self.assertIsNotNone(lineage_edge_version.reference, "lineage_edge_version attribute 'reference' is None")
        self.assertEqual(lineage_edge_version.reference, "testReference", "lineage_edge_version attribute 'reference', "
                                                                          "Expected: testReference, "
                                                                          "Actual: " + str(
            lineage_edge_version.reference))
        self.assertIsNotNone(lineage_edge_version.referenceParameters, "lineage_edge_version attribute "
                                                                       "'referenceParameters' is None")
        self.assertEqual(lineage_edge_version.referenceParameters, "testReferenceParameters",
                         "lineage_edge_version "
                         "attribute "
                         "'referenceParameters', "
                         "Expected: testReferenceParameters, "
                         "Actual: " + str(lineage_edge_version.referenceParameters))
        self.assertIsNotNone(lineage_edge_version.tags, "lineage_edge_version attribute 'tags' is None")
        self.assertEqual(lineage_edge_version.tags,
                         {'testKey': 'testValue'},
                         "lineage_edge_version attribute 'tags', "
                         "Expected: " + str({'testKey': 'testValue'}) + ", Actual: " + str(lineage_edge_version.tags))
        self.assertIsNotNone(lineage_edge_version.structureVersionId,
                             "lineage_edge_version attribute 'structureVersionId' is None")
        self.assertEqual(lineage_edge_version.structureVersionId, 1,
                         "lineage_edge_version attribute 'structureVersionId', "
                         "Expected: 1, "
                         "Actual: " + str(lineage_edge_version.structureVersionId))
        self.assertIsNotNone(lineage_edge_version.parentIds, "lineage_edge_version attribute 'parentIds' is None")
        self.assertEqual(lineage_edge_version.parentIds, [2, 3], "lineage_edge_version attribute 'parentIds', "
                                                                 "Expected: [2, 3], "
                                                                 "Actual: " + str(lineage_edge_version.parentIds))
        lineage_edge_version_json = lineage_edge_version.to_json()
        self.assertEqual(lineage_edge_version_json, '{"reference": "testReference", "tags": {"testKey": "testValue"}, '
                                                    '"lineageEdgeVersionId": 1, '
                                                    '"referenceParameters": "testReferenceParameters", '
                                                    '"fromRichVersionId": 4, "class": "LineageEdgeVersion", '
                                                    '"parentIds": [2, 3], "structureVersionId": 1, '
                                                    '"toRichVersionId": 5, "lineageEdgeId": 0, '
                                                    '"sourceKey": "testSourceKey"}')

    def test_git_get_lineage_graph_version(self):
        git = ground.GitImplementation()
        lineageGraphId = git.createLineageGraph('testSourceKey', 'testName', {'testKey': 'testValue'})
        lineageGraphVersionId = git.createLineageGraphVersion(lineageGraphId, [5, 4], "testReference",
                                                              "testReferenceParameters",
                                                              {'testKey': 'testValue'}, 1, [2, 3])
        lineage_graph_version = git.getLineageGraphVersion(lineageGraphVersionId)
        self.assertIsNotNone(lineage_graph_version.sourceKey, "lineage_graph_version attribute 'sourceKey' is None")
        self.assertEqual(lineage_graph_version.sourceKey, 'testSourceKey',
                         "lineage_graph_version attribute 'sourceKey', "
                         "Expected: testSourceKey, "
                         "Actual: " + str(
                             lineage_graph_version.sourceKey))
        self.assertIsNotNone(lineage_graph_version.lineageGraphId, "lineage_graph_version attribute "
                                                                   "'lineageGraphId' is None")
        self.assertEqual(lineage_graph_version.lineageGraphId, 0, "lineage_graph_version attribute 'lineageGraphId', "
                                                                  "Expected: 0, Actual: " + str(
            lineage_graph_version.lineageGraphId))
        self.assertIsNotNone(lineage_graph_version.lineageEdgeVersionIds, "lineage_graph_version attribute "
                                                                          "'lineageEdgeVersionIds' is None")
        self.assertEqual(lineage_graph_version.lineageEdgeVersionIds, [5, 4],
                         "lineage_graph_version attribute 'lineageEdgeVersionIds', "
                         "Expected: [5, 4], Actual: " + str(lineage_graph_version.lineageEdgeVersionIds))
        self.assertIsNotNone(lineage_graph_version.reference, "lineage_graph_version attribute 'reference' is None")
        self.assertEqual(lineage_graph_version.reference, "testReference",
                         "lineage_graph_version attribute 'reference', "
                         "Expected: testReference, "
                         "Actual: " + str(
                             lineage_graph_version.reference))
        self.assertIsNotNone(lineage_graph_version.referenceParameters, "lineage_graph_version attribute "
                                                                        "'referenceParameters' is None")
        self.assertEqual(lineage_graph_version.referenceParameters, "testReferenceParameters",
                         "lineage_graph_version "
                         "attribute "
                         "'referenceParameters', "
                         "Expected: testReferenceParameters, "
                         "Actual: " + str(lineage_graph_version.referenceParameters))
        self.assertIsNotNone(lineage_graph_version.tags, "lineage_graph_version attribute 'tags' is None")
        self.assertEqual(lineage_graph_version.tags,
                         {'testKey': 'testValue'},
                         "lineage_graph_version attribute 'tags', "
                         "Expected: " + str({'testKey': 'testValue'}) + ", Actual: " + str(lineage_graph_version.tags))
        self.assertIsNotNone(lineage_graph_version.structureVersionId,
                             "lineage_graph_version attribute 'structureVersionId' is None")
        self.assertEqual(lineage_graph_version.structureVersionId, 1,
                         "lineage_graph_version attribute 'structureVersionId', "
                         "Expected: 1, "
                         "Actual: " + str(lineage_graph_version.structureVersionId))
        self.assertIsNotNone(lineage_graph_version.parentIds, "lineage_graph_version attribute 'parentIds' is None")
        self.assertEqual(lineage_graph_version.parentIds, [2, 3], "lineage_graph_version attribute 'parentIds', "
                                                                  "Expected: [2, 3], "
                                                                  "Actual: " + str(lineage_graph_version.parentIds))
        lineage_graph_version_json = lineage_graph_version.to_json()
        self.assertEqual(lineage_graph_version_json, '{"lineageGraphId": 0, "lineageGraphVersionId": 1, '
                                                     '"parentIds": [2, 3], "reference": "testReference", '
                                                     '"tags": {"testKey": "testValue"}, '
                                                     '"referenceParameters": "testReferenceParameters", '
                                                     '"lineageEdgeVersionIds": [5, 4], '
                                                     '"structureVersionId": 1, "sourceKey": "testSourceKey", '
                                                     '"class": "LineageGraphVersion"}')

    def test_git_get_node_latest_version(self):
        git = ground.GitImplementation()
        nodeId = git.createNode('testSourceKey')
        nodeVersionIdOne = git.createNodeVersion(nodeId, "testReference", "testReferenceParameters",
                                                 {'testKeyOne': 'testValueOne'}, 1)
        nodeVersionIdTwo = git.createNodeVersion(nodeId, "testReference", "testReferenceParameters",
                                                 {'testKeyTwo': 'testValueTwo'}, 1, [nodeVersionIdOne])
        nodeVersionIdThree = git.createNodeVersion(nodeId, "testReference", "testReferenceParameters",
                                                   {'testKeyThree': 'testValueThree'}, 1,
                                                   [nodeVersionIdOne, nodeVersionIdTwo])
        nodeVersionIdFour = git.createNodeVersion(nodeId, "testReference", "testReferenceParameters",
                                                  {'testKeyFour': 'testValueFour'}, 1, [nodeVersionIdTwo])
        nodeVersionIdFive = git.createNodeVersion(nodeId, "testReference", "testReferenceParameters",
                                                  {'testKeyFive': 'testValueFive'}, 1, [nodeVersionIdThree])
        nodeVersionIdSix = git.createNodeVersion(nodeId, "testReference", "testReferenceParameters",
                                                 {'testKeySix': 'testValueSix'}, 1,
                                                 [nodeVersionIdTwo, nodeVersionIdThree])
        nodeLastestIds = [nv.nodeVersionId for nv in git.getNodeLatestVersions('testSourceKey')]
        self.assertNotIn(0, nodeLastestIds)
        self.assertNotIn(1, nodeLastestIds)
        self.assertNotIn(2, nodeLastestIds)
        self.assertNotIn(3, nodeLastestIds)
        self.assertIn(4, nodeLastestIds)
        self.assertIn(5, nodeLastestIds)
        self.assertIn(6, nodeLastestIds)

    def test_git_get_edge_latest_version(self):
        git = ground.GitImplementation()
        edgeId = git.createEdge('testSourceKey', 0, 10)
        edgeVersionIdOne = git.createEdgeVersion(edgeId, 4, 5, 6, 7, "testReference", "testReferenceParameters",
                                                 {'testKey': 'testValue'}, 1)
        edgeVersionIdTwo = git.createEdgeVersion(edgeId, 4, 5, 6, 7, "testReference", "testReferenceParameters",
                                                 {'testKey': 'testValue'}, 1, [edgeVersionIdOne])
        edgeVersionIdThree = git.createEdgeVersion(edgeId, 4, 5, 6, 7, "testReference", "testReferenceParameters",
                                                   {'testKey': 'testValue'}, 1, [edgeVersionIdOne, edgeVersionIdTwo])
        edgeVersionIdFour = git.createEdgeVersion(edgeId, 4, 5, 6, 7, "testReference", "testReferenceParameters",
                                                  {'testKey': 'testValue'}, 1, [edgeVersionIdTwo])
        edgeVersionIdFive = git.createEdgeVersion(edgeId, 4, 5, 6, 7, "testReference", "testReferenceParameters",
                                                  {'testKey': 'testValue'}, 1, [edgeVersionIdThree])
        edgeVersionIdSix = git.createEdgeVersion(edgeId, 4, 5, 6, 7, "testReference", "testReferenceParameters",
                                                 {'testKey': 'testValue'}, 1, [edgeVersionIdTwo, edgeVersionIdThree])
        edgeLastestIds = [nv.edgeVersionId for nv in git.getEdgeLatestVersions('testSourceKey')]
        self.assertNotIn(0, edgeLastestIds)
        self.assertNotIn(1, edgeLastestIds)
        self.assertNotIn(2, edgeLastestIds)
        self.assertNotIn(3, edgeLastestIds)
        self.assertIn(4, edgeLastestIds)
        self.assertIn(5, edgeLastestIds)
        self.assertIn(6, edgeLastestIds)

    def test_git_get_graph_latest_version(self):
        git = ground.GitImplementation()
        graphId = git.createGraph('testSourceKey')
        graphVersionIdOne = git.createGraphVersion(graphId, [4, 5, 6], "testReference", "testReferenceParameters",
                                                   {'testKey': 'testValue'}, 1)
        graphVersionIdTwo = git.createGraphVersion(graphId, [4, 5, 6], "testReference", "testReferenceParameters",
                                                   {'testKey': 'testValue'}, 1, [graphVersionIdOne])
        graphVersionIdThree = git.createGraphVersion(graphId, [4, 5, 6], "testReference", "testReferenceParameters",
                                                     {'testKey': 'testValue'}, 1,
                                                     [graphVersionIdOne, graphVersionIdTwo])
        graphVersionIdFour = git.createGraphVersion(graphId, [4, 5, 6], "testReference", "testReferenceParameters",
                                                    {'testKey': 'testValue'}, 1, [graphVersionIdTwo])
        graphVersionIdFive = git.createGraphVersion(graphId, [4, 5, 6], "testReference", "testReferenceParameters",
                                                    {'testKey': 'testValue'}, 1, [graphVersionIdThree])
        graphVersionIdSix = git.createGraphVersion(graphId, [4, 5, 6], "testReference", "testReferenceParameters",
                                                   {'testKey': 'testValue'}, 1,
                                                   [graphVersionIdTwo, graphVersionIdThree])
        graphLastestIds = [nv.graphVersionId for nv in git.getGraphLatestVersions('testSourceKey')]
        self.assertNotIn(0, graphLastestIds)
        self.assertNotIn(1, graphLastestIds)
        self.assertNotIn(2, graphLastestIds)
        self.assertNotIn(3, graphLastestIds)
        self.assertIn(4, graphLastestIds)
        self.assertIn(5, graphLastestIds)
        self.assertIn(6, graphLastestIds)

    def test_git_get_structure_latest_version(self):
        git = ground.GitImplementation()
        structureId = git.createStructure('testSourceKey')
        structureVersionIdOne = git.createStructureVersion(structureId, {'testKey': 'testValue'})
        structureVersionIdTwo = git.createStructureVersion(structureId, {'testKey': 'testValue'},
                                                           [structureVersionIdOne])
        structureVersionIdThree = git.createStructureVersion(structureId, {'testKey': 'testValue'},
                                                             [structureVersionIdOne, structureVersionIdTwo])
        structureVersionIdFour = git.createStructureVersion(structureId, {'testKey': 'testValue'},
                                                            [structureVersionIdTwo])
        structureVersionIdFive = git.createStructureVersion(structureId, {'testKey': 'testValue'},
                                                            [structureVersionIdThree])
        structureVersionIdSix = git.createStructureVersion(structureId, {'testKey': 'testValue'},
                                                           [structureVersionIdTwo, structureVersionIdThree])
        structureLastestIds = [nv.structureVersionId for nv in git.getStructureLatestVersions('testSourceKey')]
        self.assertNotIn(0, structureLastestIds)
        self.assertNotIn(1, structureLastestIds)
        self.assertNotIn(2, structureLastestIds)
        self.assertNotIn(3, structureLastestIds)
        self.assertIn(4, structureLastestIds)
        self.assertIn(5, structureLastestIds)
        self.assertIn(6, structureLastestIds)

    def test_git_get_lineage_edge_latest_version(self):
        git = ground.GitImplementation()
        lineageEdgeId = git.createLineageEdge('testSourceKey')
        lineageEdgeVersionIdOne = git.createLineageEdgeVersion(lineageEdgeId, 5, 4, "testReference",
                                                               "testReferenceParameters",
                                                               {'testKey': 'testValue'}, 1)
        lineageEdgeVersionIdTwo = git.createLineageEdgeVersion(lineageEdgeId, 5, 4, "testReference",
                                                               "testReferenceParameters",
                                                               {'testKey': 'testValue'}, 1,
                                                               [lineageEdgeVersionIdOne])
        lineageEdgeVersionIdThree = git.createLineageEdgeVersion(lineageEdgeId, 5, 4, "testReference",
                                                                 "testReferenceParameters",
                                                                 {'testKey': 'testValue'}, 1,
                                                                 [lineageEdgeVersionIdOne, lineageEdgeVersionIdTwo])
        lineageEdgeVersionIdFour = git.createLineageEdgeVersion(lineageEdgeId, 5, 4, "testReference",
                                                                "testReferenceParameters",
                                                                {'testKey': 'testValue'}, 1,
                                                                [lineageEdgeVersionIdTwo])
        lineageEdgeVersionIdFive = git.createLineageEdgeVersion(lineageEdgeId, 5, 4, "testReference",
                                                                "testReferenceParameters",
                                                                {'testKey': 'testValue'}, 1,
                                                                [lineageEdgeVersionIdThree])
        lineageEdgeVersionIdSix = git.createLineageEdgeVersion(lineageEdgeId, 5, 4, "testReference",
                                                               "testReferenceParameters",
                                                               {'testKey': 'testValue'}, 1,
                                                               [lineageEdgeVersionIdTwo, lineageEdgeVersionIdThree])
        lineageEdgeLastestIds = [nv.lineageEdgeVersionId for nv in git.getLineageEdgeLatestVersions('testSourceKey')]
        self.assertNotIn(0, lineageEdgeLastestIds)
        self.assertNotIn(1, lineageEdgeLastestIds)
        self.assertNotIn(2, lineageEdgeLastestIds)
        self.assertNotIn(3, lineageEdgeLastestIds)
        self.assertIn(4, lineageEdgeLastestIds)
        self.assertIn(5, lineageEdgeLastestIds)
        self.assertIn(6, lineageEdgeLastestIds)

    def test_git_get_lineage_graph_latest_version(self):
        git = ground.GitImplementation()
        lineageGraphId = git.createLineageGraph('testSourceKey')
        lineageGraphVersionIdOne = git.createLineageGraphVersion(lineageGraphId, [5, 4], "testReference",
                                                                 "testReferenceParameters",
                                                                 {'testKey': 'testValue'}, 1)
        lineageGraphVersionIdTwo = git.createLineageGraphVersion(lineageGraphId, [5, 4], "testReference",
                                                                 "testReferenceParameters",
                                                                 {'testKey': 'testValue'}, 1,
                                                                 [lineageGraphVersionIdOne])
        lineageGraphVersionIdThree = git.createLineageGraphVersion(lineageGraphId, [5, 4], "testReference",
                                                                   "testReferenceParameters",
                                                                   {'testKey': 'testValue'}, 1,
                                                                   [lineageGraphVersionIdOne, lineageGraphVersionIdTwo])
        lineageGraphVersionIdFour = git.createLineageGraphVersion(lineageGraphId, [5, 4], "testReference",
                                                                  "testReferenceParameters",
                                                                  {'testKey': 'testValue'}, 1,
                                                                  [lineageGraphVersionIdTwo])
        lineageGraphVersionIdFive = git.createLineageGraphVersion(lineageGraphId, [5, 4], "testReference",
                                                                  "testReferenceParameters",
                                                                  {'testKey': 'testValue'}, 1,
                                                                  [lineageGraphVersionIdThree])
        lineageGraphVersionIdSix = git.createLineageGraphVersion(lineageGraphId, [5, 4], "testReference",
                                                                 "testReferenceParameters",
                                                                 {'testKey': 'testValue'}, 1,
                                                                 [lineageGraphVersionIdTwo, lineageGraphVersionIdThree])
        lineageGraphLastestIds = [nv.lineageGraphVersionId for nv in git.getLineageGraphLatestVersions('testSourceKey')]
        self.assertNotIn(0, lineageGraphLastestIds)
        self.assertNotIn(1, lineageGraphLastestIds)
        self.assertNotIn(2, lineageGraphLastestIds)
        self.assertNotIn(3, lineageGraphLastestIds)
        self.assertIn(4, lineageGraphLastestIds)
        self.assertIn(5, lineageGraphLastestIds)
        self.assertIn(6, lineageGraphLastestIds)

    def test_git_get_node_history(self):
        git = ground.GitImplementation()
        nodeId = git.createNode('testSourceKey')
        nodeVersionIdOne = git.createNodeVersion(nodeId, "testReference", "testReferenceParameters",
                                                 {'testKeyOne': 'testValueOne'}, 1)
        nodeVersionIdTwo = git.createNodeVersion(nodeId, "testReference", "testReferenceParameters",
                                                 {'testKeyTwo': 'testValueTwo'}, 1, [nodeVersionIdOne])
        nodeVersionIdThree = git.createNodeVersion(nodeId, "testReference", "testReferenceParameters",
                                                   {'testKeyThree': 'testValueThree'}, 1,
                                                   [nodeVersionIdOne, nodeVersionIdTwo])
        nodeVersionIdFour = git.createNodeVersion(nodeId, "testReference", "testReferenceParameters",
                                                  {'testKeyFour': 'testValueFour'}, 1, [nodeVersionIdTwo])
        nodeVersionIdFive = git.createNodeVersion(nodeId, "testReference", "testReferenceParameters",
                                                  {'testKeyFive': 'testValueFive'}, 1, [nodeVersionIdThree])
        nodeVersionIdSix = git.createNodeVersion(nodeId, "testReference", "testReferenceParameters",
                                                 {'testKeySix': 'testValueSix'}, 1,
                                                 [nodeVersionIdThree])
        self.assertEqual(git.getNodeHistory('testSourceKey'), {'0': 1, '1': 3, '3': 6, '2': 4})

    def test_git_get_edge_history(self):
        git = ground.GitImplementation()
        edgeId = git.createEdge('testSourceKey', 0, 10)
        edgeVersionIdOne = git.createEdgeVersion(edgeId, 4, 5, 6, 7, "testReference", "testReferenceParameters",
                                                 {'testKey': 'testValue'}, 1)
        edgeVersionIdTwo = git.createEdgeVersion(edgeId, 4, 5, 6, 7, "testReference", "testReferenceParameters",
                                                 {'testKey': 'testValue'}, 1, [edgeVersionIdOne])
        edgeVersionIdThree = git.createEdgeVersion(edgeId, 4, 5, 6, 7, "testReference", "testReferenceParameters",
                                                   {'testKey': 'testValue'}, 1, [edgeVersionIdOne, edgeVersionIdTwo])
        edgeVersionIdFour = git.createEdgeVersion(edgeId, 4, 5, 6, 7, "testReference", "testReferenceParameters",
                                                  {'testKey': 'testValue'}, 1, [edgeVersionIdTwo])
        edgeVersionIdFive = git.createEdgeVersion(edgeId, 4, 5, 6, 7, "testReference", "testReferenceParameters",
                                                  {'testKey': 'testValue'}, 1, [edgeVersionIdThree])
        edgeVersionIdSix = git.createEdgeVersion(edgeId, 4, 5, 6, 7, "testReference", "testReferenceParameters",
                                                 {'testKey': 'testValue'}, 1, [edgeVersionIdThree])
        self.assertEqual(git.getEdgeHistory('testSourceKey'), {'0': 1, '1': 3, '3': 6, '2': 4})

    def test_git_get_graph_history(self):
        git = ground.GitImplementation()
        graphId = git.createGraph('testSourceKey')
        graphVersionIdOne = git.createGraphVersion(graphId, [4, 5, 6], "testReference", "testReferenceParameters",
                                                   {'testKey': 'testValue'}, 1)
        graphVersionIdTwo = git.createGraphVersion(graphId, [4, 5, 6], "testReference", "testReferenceParameters",
                                                   {'testKey': 'testValue'}, 1, [graphVersionIdOne])
        graphVersionIdThree = git.createGraphVersion(graphId, [4, 5, 6], "testReference", "testReferenceParameters",
                                                     {'testKey': 'testValue'}, 1,
                                                     [graphVersionIdOne, graphVersionIdTwo])
        graphVersionIdFour = git.createGraphVersion(graphId, [4, 5, 6], "testReference", "testReferenceParameters",
                                                    {'testKey': 'testValue'}, 1, [graphVersionIdTwo])
        graphVersionIdFive = git.createGraphVersion(graphId, [4, 5, 6], "testReference", "testReferenceParameters",
                                                    {'testKey': 'testValue'}, 1, [graphVersionIdThree])
        graphVersionIdSix = git.createGraphVersion(graphId, [4, 5, 6], "testReference", "testReferenceParameters",
                                                   {'testKey': 'testValue'}, 1,
                                                   [graphVersionIdThree])
        self.assertEqual(git.getGraphHistory('testSourceKey'), {'0': 1, '1': 3, '3': 6, '2': 4})

    def test_git_get_structure_history(self):
        git = ground.GitImplementation()
        structureId = git.createStructure('testSourceKey')
        structureVersionIdOne = git.createStructureVersion(structureId, {'testKey': 'testValue'})
        structureVersionIdTwo = git.createStructureVersion(structureId, {'testKey': 'testValue'},
                                                           [structureVersionIdOne])
        structureVersionIdThree = git.createStructureVersion(structureId, {'testKey': 'testValue'},
                                                             [structureVersionIdOne, structureVersionIdTwo])
        structureVersionIdFour = git.createStructureVersion(structureId, {'testKey': 'testValue'},
                                                            [structureVersionIdTwo])
        structureVersionIdFive = git.createStructureVersion(structureId, {'testKey': 'testValue'},
                                                            [structureVersionIdThree])
        structureVersionIdSix = git.createStructureVersion(structureId, {'testKey': 'testValue'},
                                                           [structureVersionIdThree])
        self.assertEqual(git.getStructureHistory('testSourceKey'), {'0': 1, '1': 3, '3': 6, '2': 4})

    def test_git_get_lineage_edge_history(self):
        git = ground.GitImplementation()
        lineageEdgeId = git.createLineageEdge('testSourceKey')
        lineageEdgeVersionIdOne = git.createLineageEdgeVersion(lineageEdgeId, 5, 4, "testReference",
                                                               "testReferenceParameters",
                                                               {'testKey': 'testValue'}, 1)
        lineageEdgeVersionIdTwo = git.createLineageEdgeVersion(lineageEdgeId, 5, 4, "testReference",
                                                               "testReferenceParameters",
                                                               {'testKey': 'testValue'}, 1,
                                                               [lineageEdgeVersionIdOne])
        lineageEdgeVersionIdThree = git.createLineageEdgeVersion(lineageEdgeId, 5, 4, "testReference",
                                                                 "testReferenceParameters",
                                                                 {'testKey': 'testValue'}, 1,
                                                                 [lineageEdgeVersionIdOne, lineageEdgeVersionIdTwo])
        lineageEdgeVersionIdFour = git.createLineageEdgeVersion(lineageEdgeId, 5, 4, "testReference",
                                                                "testReferenceParameters",
                                                                {'testKey': 'testValue'}, 1,
                                                                [lineageEdgeVersionIdTwo])
        lineageEdgeVersionIdFive = git.createLineageEdgeVersion(lineageEdgeId, 5, 4, "testReference",
                                                                "testReferenceParameters",
                                                                {'testKey': 'testValue'}, 1,
                                                                [lineageEdgeVersionIdThree])
        lineageEdgeVersionIdSix = git.createLineageEdgeVersion(lineageEdgeId, 5, 4, "testReference",
                                                               "testReferenceParameters",
                                                               {'testKey': 'testValue'}, 1,
                                                               [lineageEdgeVersionIdThree])
        self.assertEqual(git.getLineageEdgeHistory('testSourceKey'), {'0': 1, '1': 3, '3': 6, '2': 4})

    def test_git_get_lineage_graph_history(self):
        git = ground.GitImplementation()
        lineageGraphId = git.createLineageGraph('testSourceKey')
        lineageGraphVersionIdOne = git.createLineageGraphVersion(lineageGraphId, [5, 4], "testReference",
                                                                 "testReferenceParameters",
                                                                 {'testKey': 'testValue'}, 1)
        lineageGraphVersionIdTwo = git.createLineageGraphVersion(lineageGraphId, [5, 4], "testReference",
                                                                 "testReferenceParameters",
                                                                 {'testKey': 'testValue'}, 1,
                                                                 [lineageGraphVersionIdOne])
        lineageGraphVersionIdThree = git.createLineageGraphVersion(lineageGraphId, [5, 4], "testReference",
                                                                   "testReferenceParameters",
                                                                   {'testKey': 'testValue'}, 1,
                                                                   [lineageGraphVersionIdOne, lineageGraphVersionIdTwo])
        lineageGraphVersionIdFour = git.createLineageGraphVersion(lineageGraphId, [5, 4], "testReference",
                                                                  "testReferenceParameters",
                                                                  {'testKey': 'testValue'}, 1,
                                                                  [lineageGraphVersionIdTwo])
        lineageGraphVersionIdFive = git.createLineageGraphVersion(lineageGraphId, [5, 4], "testReference",
                                                                  "testReferenceParameters",
                                                                  {'testKey': 'testValue'}, 1,
                                                                  [lineageGraphVersionIdThree])
        lineageGraphVersionIdSix = git.createLineageGraphVersion(lineageGraphId, [5, 4], "testReference",
                                                                 "testReferenceParameters",
                                                                 {'testKey': 'testValue'}, 1,
                                                                 [lineageGraphVersionIdThree])
        self.assertEqual(git.getLineageGraphHistory('testSourceKey'), {'0': 1, '1': 3, '3': 6, '2': 4})

    def test_git_get_node_version_adjacent_history(self):
        git = ground.GitImplementation()
        nodeId = git.createNode('testSourceKey')
        nodeVersionIdOne = git.createNodeVersion(nodeId, "testReference", "testReferenceParameters",
                                                 {'testKeyOne': 'testValueOne'}, 1)
        nodeVersionIdTwo = git.createNodeVersion(nodeId, "testReference", "testReferenceParameters",
                                                 {'testKeyTwo': 'testValueTwo'}, 1, [nodeVersionIdOne])
        nodeVersionIdThree = git.createNodeVersion(nodeId, "testReference", "testReferenceParameters",
                                                   {'testKeyThree': 'testValueThree'}, 1,
                                                   [nodeVersionIdOne, nodeVersionIdTwo])
        nodeVersionIdFour = git.createNodeVersion(nodeId, "testReference", "testReferenceParameters",
                                                  {'testKeyFour': 'testValueFour'}, 1, [nodeVersionIdTwo])
        nodeVersionIdFive = git.createNodeVersion(nodeId, "testReference", "testReferenceParameters",
                                                  {'testKeyFive': 'testValueFive'}, 1, [nodeVersionIdThree])
        nodeVersionIdSix = git.createNodeVersion(nodeId, "testReference", "testReferenceParameters",
                                                 {'testKeySix': 'testValueSix'}, 1,
                                                 [nodeVersionIdThree])
        lineageEdgeId = git.createLineageEdge('testSourceKey')
        lineageEdgeVersionIdOne = git.createLineageEdgeVersion(lineageEdgeId, 5, 3, "testReference",
                                                               "testReferenceParameters",
                                                               {'testKey': 'testValue'}, 1)
        lineageEdgeVersionIdTwo = git.createLineageEdgeVersion(lineageEdgeId, 3, 2, "testReference",
                                                               "testReferenceParameters",
                                                               {'testKey': 'testValue'}, 1,
                                                               [lineageEdgeVersionIdOne])
        lineageEdgeVersionIdThree = git.createLineageEdgeVersion(lineageEdgeId, 4, 1, "testReference",
                                                                 "testReferenceParameters",
                                                                 {'testKey': 'testValue'}, 1,
                                                                 [lineageEdgeVersionIdOne, lineageEdgeVersionIdTwo])
        lineageEdgeVersionIdFour = git.createLineageEdgeVersion(lineageEdgeId, 5, 2, "testReference",
                                                                "testReferenceParameters",
                                                                {'testKey': 'testValue'}, 1,
                                                                [lineageEdgeVersionIdTwo])
        lineageEdgeVersionIdFive = git.createLineageEdgeVersion(lineageEdgeId, 6, 4, "testReference",
                                                                "testReferenceParameters",
                                                                {'testKey': 'testValue'}, 1,
                                                                [lineageEdgeVersionIdThree])
        lineageEdgeVersionIdSix = git.createLineageEdgeVersion(lineageEdgeId, 6, 1, "testReference",
                                                               "testReferenceParameters",
                                                               {'testKey': 'testValue'}, 1,
                                                               [lineageEdgeVersionIdThree])
        adjIdsOne = [adj.lineageEdgeVersionId for adj in git.getNodeVersionAdjacentLineage(nodeVersionIdOne)]
        self.assertEqual(2, len(adjIdsOne))
        self.assertIn(10, adjIdsOne)
        self.assertIn(13, adjIdsOne)
        adjIdsTwo = [adj.lineageEdgeVersionId for adj in git.getNodeVersionAdjacentLineage(nodeVersionIdTwo)]
        self.assertEqual(2, len(adjIdsTwo))
        self.assertIn(9, adjIdsTwo)
        self.assertIn(11, adjIdsTwo)
        adjIdsThree = [adj.lineageEdgeVersionId for adj in git.getNodeVersionAdjacentLineage(nodeVersionIdThree)]
        self.assertEqual(2, len(adjIdsThree))
        self.assertIn(8, adjIdsThree)
        self.assertIn(9, adjIdsThree)
        adjIdsFour = [adj.lineageEdgeVersionId for adj in git.getNodeVersionAdjacentLineage(nodeVersionIdFour)]
        self.assertEqual(2, len(adjIdsFour))
        self.assertIn(10, adjIdsFour)
        self.assertIn(12, adjIdsFour)
        adjIdsFive = [adj.lineageEdgeVersionId for adj in git.getNodeVersionAdjacentLineage(nodeVersionIdFive)]
        self.assertEqual(2, len(adjIdsFive))
        self.assertIn(8, adjIdsFive)
        self.assertIn(11, adjIdsFive)
        adjIdsSix = [adj.lineageEdgeVersionId for adj in git.getNodeVersionAdjacentLineage(nodeVersionIdSix)]
        self.assertEqual(2, len(adjIdsSix))
        self.assertIn(12, adjIdsSix)
        self.assertIn(13, adjIdsSix)


if __name__ == '__main__':
    unittest.main()
