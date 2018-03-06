# /usr/bin/env python3
import requests
import json
import numpy as np
import os
import git
import subprocess
from shutil import copyfile


class Node:
    def __init__(self, sourceKey=None, name=None, tags=None):
        self.sourceKey = sourceKey
        self.tags = tags
        self.name = name
        self.nodeId = None

    def to_json(self):
        d = {
            'sourceKey': self.sourceKey,
            'tags': self.tags,
            'name': self.name,
            'nodeId': self.nodeId,
            'class': 'Node'
        }
        return json.dumps(d)


class NodeVersion:
    def __init__(self, node=Node(), reference=None, referenceParameters=None, tags=None,
                 structureVersionId=None, parentIds=None):
        self.sourceKey = node.sourceKey
        self.nodeId = node.nodeId
        self.reference = reference
        self.referenceParameters = referenceParameters
        self.tags = tags
        self.structureVersionId = structureVersionId
        self.parentIds = parentIds
        self.nodeVersionId = None

    def to_json(self):
        d = {
            'sourceKey': self.sourceKey,
            'nodeId': self.nodeId,
            'reference': self.reference,
            'referenceParameters': self.referenceParameters,
            'tags': self.tags,
            'structureVersionId': self.structureVersionId,
            'parentIds': self.parentIds,
            'nodeVersionId': self.nodeVersionId,
            'class': 'NodeVersion'
        }
        return json.dumps(d)


class Edge:
    def __init__(self, sourceKey=None, fromNodeId=None, toNodeId=None, name=None, tags=None):
        self.sourceKey = sourceKey
        self.fromNodeId = fromNodeId
        self.toNodeId = toNodeId
        self.name = name
        self.tags = tags
        self.edgeId = None

    def to_json(self):
        d = {
            'sourceKey': self.sourceKey,
            'fromNodeId': self.fromNodeId,
            'toNodeId': self.toNodeId,
            'name': self.name,
            'tags': self.tags,
            'edgeId': self.edgeId,
            'class': 'Edge'
        }
        return json.dumps(d)


class EdgeVersion:
    def __init__(self, edge=Edge(), toNodeVersionStartId=None, fromNodeVersionStartId=None, toNodeVersionEndId=None,
                 fromNodeVersionEndId=None, reference=None, referenceParameters=None,
                 tags=None, structureVersionId=None, parentIds=None):
        self.sourceKey = edge.sourceKey
        self.fromNodeId = edge.fromNodeId
        self.toNodeId = edge.toNodeId
        self.edgeId = edge.edgeId
        self.toNodeVersionStartId = toNodeVersionStartId
        self.fromNodeVersionStartId = fromNodeVersionStartId
        self.toNodeVersionEndId = toNodeVersionEndId
        self.fromNodeVersionEndId = fromNodeVersionEndId
        self.reference = reference
        self.referenceParameters = referenceParameters
        self.tags = tags
        self.structureVersionId = structureVersionId
        self.parentIds = parentIds
        self.edgeVersionId = None

    def to_json(self):
        d = {
            'sourceKey': self.sourceKey,
            'fromNodeId': self.fromNodeId,
            'toNodeId': self.toNodeId,
            'edgeId': self.edgeId,
            'toNodeVersionStartId': self.toNodeVersionStartId,
            'fromNodeVersionStartId': self.fromNodeVersionStartId,
            'toNodeVersionEndId': self.toNodeVersionEndId,
            'fromNodeVersionEndId': self.fromNodeVersionEndId,
            'reference': self.reference,
            'referenceParameters': self.referenceParameters,
            'tags': self.tags,
            'structureVersionId': self.structureVersionId,
            'parentIds': self.parentIds,
            'edgeVersionId': self.edgeVersionId,
            'class': 'EdgeVersion'
        }
        return json.dumps(d)


class Graph:
    def __init__(self, sourceKey=None, name=None, tags=None):
        self.sourceKey = sourceKey
        self.name = name
        self.tags = tags
        self.graphId = None

        self.nodes = {}
        self.nodeVersions = {}
        self.edges = {}
        self.edgeVersions = {}
        self.graphs = {}
        self.graphVersions = {}
        self.structures = {}
        self.structureVersions = {}
        self.lineageEdges = {}
        self.lineageEdgeVersions = {}
        self.lineageGraphs = {}
        self.lineageGraphVersions = {}
        self.ids = set([])

        self.__loclist__ = []
        self.__scriptNames__ = []

    def gen_id(self):
        newid = len(self.ids)
        self.ids |= {newid}
        return newid

    def to_json(self):
        d = {
            'sourceKey': self.sourceKey,
            'tags': self.tags,
            'name': self.name,
            'graphId': self.graphId,
            'class': 'Graph'
        }
        return json.dumps(d)


class GraphVersion:
    def __init__(self, graph=Graph(), edgeVersionIds=None, reference=None, referenceParameters=None,
                 tags=None, structureVersionId=None, parentIds=None):
        self.sourceKey = graph.sourceKey
        self.graphId = graph.graphId
        self.edgeVersionIds = edgeVersionIds
        self.structureVersionId = structureVersionId
        self.reference = reference
        self.referenceParameters = referenceParameters
        self.tags = tags
        self.parentIds = parentIds
        self.graphVersionId = None

    def to_json(self):
        d = {
            'sourceKey': self.sourceKey,
            'graphId': self.graphId,
            'edgeVersionIds': self.edgeVersionIds,
            'structureVersionId': self.structureVersionId,
            'reference': self.reference,
            'referenceParameters': self.referenceParameters,
            'tags': self.tags,
            'parentIds': self.parentIds,
            'graphVersionId': self.graphVersionId,
            'class': 'GraphVersion'
        }
        return json.dumps(d)


class Structure:
    def __init__(self, sourceKey=None, name=None, tags=None):
        self.sourceKey = sourceKey
        self.name = name
        self.tags = tags
        self.structureId = None

    def to_json(self):
        d = {
            'sourceKey': self.sourceKey,
            'tags': self.tags,
            'name': self.name,
            'structureId': self.structureId,
            'class': 'Structure'
        }
        return json.dumps(d)


class StructureVersion:
    def __init__(self, structure=Structure(), attributes=None, parentIds=None):
        self.sourceKey = structure.sourceKey
        self.structureId = structure.structureId
        self.attributes = attributes
        self.parentIds = parentIds
        self.structureVersionId = None

    def to_json(self):
        d = {
            'sourceKey': self.sourceKey,
            'structureId': self.structureId,
            'attributes': self.attributes,
            'parentIds': self.parentIds,
            'structureVersionId': self.structureVersionId,
            'class': 'StructureVersion'
        }
        return json.dumps(d)


class LineageEdge:
    def __init__(self, sourceKey=None, name=None, tags=None):
        self.sourceKey = sourceKey
        self.name = name
        self.tags = tags
        self.lineageEdgeId = None

    def to_json(self):
        d = {
            'sourceKey': self.sourceKey,
            'name': self.name,
            'tags': self.tags,
            'lineageEdgeId': self.lineageEdgeId,
            'class': 'LineageEdge'
        }
        return json.dumps(d)


class LineageEdgeVersion:
    def __init__(self, lineageEdge=LineageEdge(), toRichVersionId=None, fromRichVersionId=None, reference=None,
                 referenceParameters=None, tags=None, structureVersionId=None, parentIds=None):
        self.sourceKey = lineageEdge.sourceKey
        self.lineageEdgeId = lineageEdge.lineageEdgeId
        self.toRichVersionId = toRichVersionId
        self.fromRichVersionId = fromRichVersionId
        self.reference = reference
        self.referenceParameters = referenceParameters
        self.tags = tags
        self.structureVersionId = structureVersionId
        self.parentIds = parentIds
        self.lineageEdgeVersionId = None

    def to_json(self):
        d = {
            'sourceKey': self.sourceKey,
            'lineageEdgeId': self.lineageEdgeId,
            'fromRichVersionId': self.fromRichVersionId,
            'toRichVersionId': self.toRichVersionId,
            'reference': self.reference,
            'referenceParameters': self.referenceParameters,
            'tags': self.tags,
            'structureVersionId': self.structureVersionId,
            'parentIds': self.parentIds,
            'lineageEdgeVersionId': self.lineageEdgeVersionId,
            'class': 'LineageEdgeVersion'
        }
        return json.dumps(d)


class LineageGraph:
    def __init__(self, sourceKey=None, name=None, tags=None):
        self.sourceKey = sourceKey
        self.name = name
        self.tags = tags
        self.lineageGraphId = None

    def to_json(self):
        d = {
            'sourceKey': self.sourceKey,
            'name': self.name,
            'tags': self.tags,
            'lineageGraphId': self.lineageGraphId,
            'class': 'LineageGraph'
        }
        return json.dumps(d)


class LineageGraphVersion:
    def __init__(self, lineageGraph=LineageGraph(), lineageEdgeVersionIds=None, reference=None,
                 referenceParameters=None,
                 tags=None, structureVersionId=None, parentIds=None):
        self.sourceKey = lineageGraph.sourceKey
        self.lineageGraphId = lineageGraph.lineageGraphId
        self.lineageEdgeVersionIds = lineageEdgeVersionIds
        self.reference = reference
        self.referenceParameters = referenceParameters
        self.tags = tags
        self.structureVersionId = structureVersionId
        self.parentIds = parentIds
        self.lineageGraphVersionId = None

    def to_json(self):
        d = {
            'sourceKey': self.sourceKey,
            'lineageGraphId': self.lineageGraphId,
            'lineageEdgeVersionIds': self.lineageEdgeVersionIds,
            'reference': self.reference,
            'referenceParameters': self.referenceParameters,
            'tags': self.tags,
            'structureVersionId': self.structureVersionId,
            'parentIds': self.parentIds,
            'lineageGraphVersionId': self.lineageGraphVersionId,
            'class': 'LineageGraphVersion'
        }
        return json.dumps(d)


"""
Abstract class: do not instantiate
"""


class GroundAPI:
    headers = {"Content-type": "application/json"}

    ### EDGES ###
    def createEdge(self, sourceKey, fromNodeId, toNodeId, name="null", tags=None):
        d = {
            "sourceKey": sourceKey,
            "fromNodeId": fromNodeId,
            "toNodeId": toNodeId,
            "name": name
        }
        if tags is not None:
            d["tags"] = tags
        return d

    def createEdgeVersion(self, edgeId, toNodeVersionStartId, fromNodeVersionStartId, toNodeVersionEndId=None,
                          fromNodeVersionEndId=None, reference=None, referenceParameters=None, tags=None,
                          structureVersionId=None, parentIds=None):
        d = {
            "edgeId": edgeId,
            "fromNodeVersionStartId": fromNodeVersionStartId,
            "toNodeVersionStartId": toNodeVersionStartId
        }
        if toNodeVersionEndId is not None:
            d["toNodeVersionEndId"] = toNodeVersionEndId
        if fromNodeVersionEndId is not None:
            d["fromNodeVersionEndId"] = fromNodeVersionEndId
        if reference is not None:
            d["reference"] = reference
        if referenceParameters is not None:
            d["referenceParameters"] = referenceParameters
        if tags is not None:
            d["tags"] = tags
        if structureVersionId is not None:
            d["structureVersionId"] = structureVersionId
        if parentIds is not None:
            d["parentIds"] = parentIds
        return d

    def getEdge(self, sourceKey):
        raise NotImplementedError("Invalid call to GroundClient.getEdge")

    def getEdgeLatestVersions(self, sourceKey):
        raise NotImplementedError(
            "Invalid call to GroundClient.getEdgeLatestVersions")

    def getEdgeHistory(self, sourceKey):
        raise NotImplementedError(
            "Invalid call to GroundClient.getEdgeHistory")

    def getEdgeVersion(self, edgeId):
        raise NotImplementedError(
            "Invalid call to GroundClient.getEdgeVersion")

    ### NODES ###
    def createNode(self, sourceKey, name="null", tags=None):
        d = {
            "sourceKey": sourceKey,
            "name": name
        }
        if tags is not None:
            d["tags"] = tags
        return d

    def createNodeVersion(self, nodeId, reference=None, referenceParameters=None, tags=None,
                          structureVersionId=None, parentIds=None):
        d = {
            "nodeId": nodeId
        }
        if reference is not None:
            d["reference"] = reference
        if referenceParameters is not None:
            d["referenceParameters"] = referenceParameters
        if tags is not None:
            d["tags"] = tags
        if structureVersionId is not None:
            d["structureVersionId"] = structureVersionId
        if parentIds is not None:
            d["parentIds"] = parentIds
        return d

    def getNode(self, sourceKey):
        raise NotImplementedError("Invalid call to GroundClient.getNode")

    def getNodeLatestVersions(self, sourceKey):
        raise NotImplementedError(
            "Invalid call to GroundClient.getNodeLatestVersions")

    def getNodeHistory(self, sourceKey):
        raise NotImplementedError(
            "Invalid call to GroundClient.getNodeHistory")

    def getNodeVersion(self, nodeId):
        raise NotImplementedError(
            "Invalid call to GroundClient.getNodeVersion")

    def getNodeVersionAdjacentLineage(self, nodeId):
        raise NotImplementedError(
            "Invalid call to GroundClient.getNodeVersionAdjacentLineage")

    ### GRAPHS ###
    def createGraph(self, sourceKey, name="null", tags=None):
        d = {
            "sourceKey": sourceKey,
            "name": name
        }
        if tags is not None:
            d["tags"] = tags
        return d

    def createGraphVersion(self, graphId, edgeVersionIds, reference=None, referenceParameters=None,
                           tags=None, structureVersionId=None, parentIds=None):
        d = {
            "graphId": graphId,
            "edgeVersionIds": edgeVersionIds
        }
        if reference is not None:
            d["reference"] = reference
        if referenceParameters is not None:
            d["referenceParameters"] = referenceParameters
        if tags is not None:
            d["tags"] = tags
        if structureVersionId is not None:
            d["structureVersionId"] = structureVersionId
        if parentIds is not None:
            d["parentIds"] = parentIds
        return d

    def getGraph(self, sourceKey):
        raise NotImplementedError("Invalid call to GroundClient.getGraph")

    def getGraphLatestVersions(self, sourceKey):
        raise NotImplementedError(
            "Invalid call to GroundClient.getGraphLatestVersions")

    def getGraphHistory(self, sourceKey):
        raise NotImplementedError(
            "Invalid call to GroundClient.getGraphHistory")

    def getGraphVersion(self, graphId):
        raise NotImplementedError(
            "Invalid call to GroundClient.getGraphVersion")

    ### STRUCTURES ###
    def createStructure(self, sourceKey, name="null", tags=None):
        d = {
            "sourceKey": sourceKey,
            "name": name
        }
        if tags is not None:
            d["tags"] = tags
        return d

    def createStructureVersion(self, structureId, attributes, parentIds=None):
        d = {
            "structureId": structureId,
            "attributes": attributes
        }
        if parentIds is not None:
            d["parentIds"] = parentIds
        return d

    def getStructure(self, sourceKey):
        raise NotImplementedError("Invalid call to GroundClient.getStructure")

    def getStructureLatestVersions(self, sourceKey):
        raise NotImplementedError(
            "Invalid call to GroundClient.getStructureLatestVersions")

    def getStructureHistory(self, sourceKey):
        raise NotImplementedError(
            "Invalid call to GroundClient.getStructureHistory")

    def getStructureVersion(self, structureId):
        raise NotImplementedError(
            "Invalid call to GroundClient.getStructureVersion")

    ### LINEAGE EDGES ###
    def createLineageEdge(self, sourceKey, name="null", tags=None):
        d = {
            "sourceKey": sourceKey,
            "name": name
        }
        if tags is not None:
            d["tags"] = tags
        return d

    def createLineageEdgeVersion(self, lineageEdgeId, toRichVersionId, fromRichVersionId, reference=None,
                                 referenceParameters=None, tags=None, structureVersionId=None, parentIds=None):
        d = {
            "lineageEdgeId": lineageEdgeId,
            "toRichVersionId": toRichVersionId,
            "fromRichVersionId": fromRichVersionId
        }
        if reference is not None:
            d["reference"] = reference
        if referenceParameters is not None:
            d["referenceParameters"] = referenceParameters
        if tags is not None:
            d["tags"] = tags
        if structureVersionId is not None:
            d["structureVersionId"] = structureVersionId
        if parentIds is not None:
            d["parentIds"] = parentIds
        return d

    def getLineageEdge(self, sourceKey):
        raise NotImplementedError("Invalid call to GroundClient.getLineageEdge")

    def getLineageEdgeLatestVersions(self, sourceKey):
        raise NotImplementedError(
            "Invalid call to GroundClient.getLineageEdgeLatestVersions")

    def getLineageEdgeHistory(self, sourceKey):
        raise NotImplementedError(
            "Invalid call to GroundClient.getLineageEdgeHistory")

    def getLineageEdgeVersion(self, lineageEdgeId):
        raise NotImplementedError(
            "Invalid call to GroundClient.getLineageEdgeVersion")

    ### LINEAGE GRAPHS ###
    def createLineageGraph(self, sourceKey, name="null", tags=None):
        d = {
            "sourceKey": sourceKey,
            "name": name
        }
        if tags is not None:
            d["tags"] = tags
        return d

    def createLineageGraphVersion(self, lineageGraphId, lineageEdgeVersionIds, reference=None,
                                  referenceParameters=None, tags=None, structureVersionId=None, parentIds=None):
        d = {
            "lineageGraphId": lineageGraphId,
            "lineageEdgeVersionIds": lineageEdgeVersionIds
        }
        if reference is not None:
            d["reference"] = reference
        if referenceParameters is not None:
            d["referenceParameters"] = referenceParameters
        if tags is not None:
            d["tags"] = tags
        if structureVersionId is not None:
            d["structureVersionId"] = structureVersionId
        if parentIds is not None:
            d["parentIds"] = parentIds
        return d

    def getLineageGraph(self, sourceKey):
        raise NotImplementedError("Invalid call to GroundClient.getLineageGraph")

    def getLineageGraphLatestVersions(self, sourceKey):
        raise NotImplementedError(
            "Invalid call to GroundClient.getLineageGraphLatestVersions")

    def getLineageGraphHistory(self, sourceKey):
        raise NotImplementedError(
            "Invalid call to GroundClient.getLineageGraphHistory")

    def getLineageGraphVersion(self, lineageGraphId):
        raise NotImplementedError(
            "Invalid call to GroundClient.getLineageGraphVersion")

    def commit(self, directory=None):
        return

    def load(self, directory):
        """
        This method is implemented by GitImplementation
        It is used to load the Ground Graph to memory (from filesystem)
        """
        return


class GitImplementation(GroundAPI):
    def __init__(self):
        self.graph = Graph()

    def __run_proc__(self, bashCommand):
        process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
        output, error = process.communicate()
        return str(output, 'UTF-8')

        ### EDGES ###

    def createEdge(self, sourceKey, fromNodeId, toNodeId, name="null", tags=None):
        if sourceKey not in self.graph.edges:
            edgeid = self.graph.gen_id()
            edge = Edge(sourceKey, fromNodeId, toNodeId, name, tags)
            edge.edgeId = edgeid

            self.graph.edges[sourceKey] = edge
            self.graph.edges[edgeid] = edge
        else:
            edgeid = self.graph.edges[sourceKey].edgeId

        return edgeid

    def createEdgeVersion(self, edgeId, toNodeVersionStartId, fromNodeVersionStartId, toNodeVersionEndId=None,
                          fromNodeVersionEndId=None, reference=None, referenceParameters=None, tags=None,
                          structureVersionId=None, parentIds=None):
        edge = Edge()
        if (edgeId in self.graph.edges):
            edge = self.graph.edges[edgeId]
        edgeVersion = EdgeVersion(edge, toNodeVersionStartId, fromNodeVersionStartId, toNodeVersionEndId,
                                  fromNodeVersionEndId, reference, referenceParameters, tags,
                                  structureVersionId, parentIds)

        edgeversionid = self.graph.gen_id()
        edgeVersion.edgeVersionId = edgeversionid

        if edgeVersion.sourceKey in self.graph.edgeVersions:
            self.graph.edgeVersions[edgeVersion.sourceKey].append(edgeVersion)
        else:
            self.graph.edgeVersions[edgeVersion.sourceKey] = [edgeVersion, ]
        self.graph.edgeVersions[edgeversionid] = edgeVersion
        return edgeversionid

    def getEdge(self, sourceKey):
        return self.graph.edges[sourceKey]

    def getEdgeLatestVersions(self, sourceKey):
        assert sourceKey in self.graph.edgeVersions
        edgeVersions = set(self.graph.edgeVersions[sourceKey])
        is_parent = set([])
        for ev in edgeVersions:
            if ev.parentIds:
                assert type(ev.parentIds) == list
                for parentId in ev.parentIds:
                    is_parent |= {self.graph.edgeVersions[parentId], }
        return list(edgeVersions - is_parent)

    def getEdgeHistory(self, sourceKey):
        assert sourceKey in self.graph.edgeVersions
        parentChild = {}
        for ev in self.graph.edgeVersions[sourceKey]:
            if ev.parentIds:
                assert type(ev.parentIds) == list
                for parentId in ev.parentIds:
                    if not parentChild:
                        edgeId = ev.edgeId
                        parentChild[str(edgeId)] = parentId
                    parentChild[str(parentId)] = ev.edgeVersionId
        return parentChild

    def getEdgeVersion(self, edgeVersionId):
        return self.graph.edgeVersions[edgeVersionId]

    ### NODES ###
    def createNode(self, sourceKey, name="null", tags=None):
        if sourceKey not in self.graph.nodes:
            nodeid = self.graph.gen_id()
            node = Node(sourceKey, name, tags)
            node.nodeId = nodeid

            self.graph.nodes[sourceKey] = node
            self.graph.nodes[nodeid] = node
        else:
            nodeid = self.graph.nodes[sourceKey].nodeId

        return nodeid

    def createNodeVersion(self, nodeId, reference=None, referenceParameters=None, tags=None,
                          structureVersionId=None, parentIds=None):
        node = Node()
        if (nodeId in self.graph.nodes):
            node = self.graph.nodes[nodeId]
        nodeVersion = NodeVersion(node, reference, referenceParameters, tags, structureVersionId, parentIds)
        # else:
        #     # Match node versions with the same tags.
        #     # ALERT: THIS MAY NOT GENERALIZE TO K-LIFTING
        #     nlvs = self.getNodeLatestVersions(self.graph.nodes[nodeId].sourceKey)
        #     if nlvs:
        #         nodeVersion.parentIds = nlvs
        #     else:
        #         nodeVersion.parentIds = None
        nodeversionid = self.graph.gen_id()
        nodeVersion.nodeVersionId = nodeversionid

        if nodeVersion.sourceKey in self.graph.nodeVersions:
            self.graph.nodeVersions[nodeVersion.sourceKey].append(nodeVersion)
        else:
            self.graph.nodeVersions[nodeVersion.sourceKey] = [nodeVersion, ]
        self.graph.nodeVersions[nodeversionid] = nodeVersion
        return nodeversionid

    def getNode(self, sourceKey):
        return self.graph.nodes[sourceKey]

    def getNodeLatestVersions(self, sourceKey):
        assert sourceKey in self.graph.nodeVersions
        nodeVersions = set(self.graph.nodeVersions[sourceKey])
        is_parent = set([])
        for nv in nodeVersions:
            if nv.parentIds:
                assert type(nv.parentIds) == list
                for parentId in nv.parentIds:
                    is_parent |= {self.graph.nodeVersions[parentId], }
        return list(nodeVersions - is_parent)

    def getNodeHistory(self, sourceKey):
        assert sourceKey in self.graph.nodeVersions
        parentChild = {}
        for nv in self.graph.nodeVersions[sourceKey]:
            if nv.parentIds:
                assert type(nv.parentIds) == list
                for parentId in nv.parentIds:
                    if not parentChild:
                        nodeId = nv.nodeId
                        parentChild[str(nodeId)] = parentId
                    parentChild[str(parentId)] = nv.nodeVersionId
        return parentChild

    def getNodeVersion(self, nodeVersionId):
        return self.graph.nodeVersions[nodeVersionId]

    def getNodeVersionAdjacentLineage(self, nodeVersionId):
        assert nodeVersionId in self.graph.nodeVersions
        lineageEdgeVersions = (self.graph.lineageEdgeVersions).values()
        adjacent = []
        for lev in lineageEdgeVersions:
            if isinstance(lev, LineageEdgeVersion):
                if ((nodeVersionId == lev.toRichVersionId) or (nodeVersionId == lev.fromRichVersionId)):
                    adjacent.append(lev)
        return list(set(adjacent))

    ### GRAPHS ###
    def createGraph(self, sourceKey, name="null", tags=None):
        if sourceKey not in self.graph.graphs:
            graphid = self.graph.gen_id()
            graph = Graph(sourceKey, name, tags)
            graph.graphId = graphid

            self.graph.graphs[sourceKey] = graph
            self.graph.graphs[graphid] = graph
        else:
            graphid = self.graph.graphs[sourceKey].graphId

        return graphid

    def createGraphVersion(self, graphId, edgeVersionIds, reference=None,
                           referenceParameters=None, tags=None, structureVersionId=None, parentIds=None):
        graph = Graph()
        if (graphId in self.graph.graphs):
            graph = self.graph.graphs[graphId]
        graphVersion = GraphVersion(graph, edgeVersionIds, reference,
                                    referenceParameters, tags, structureVersionId, parentIds)

        graphversionid = self.graph.gen_id()
        graphVersion.graphVersionId = graphversionid

        if graphVersion.sourceKey in self.graph.graphVersions:
            self.graph.graphVersions[graphVersion.sourceKey].append(graphVersion)
        else:
            self.graph.graphVersions[graphVersion.sourceKey] = [graphVersion, ]
        self.graph.graphVersions[graphversionid] = graphVersion
        return graphversionid

    def getGraph(self, sourceKey):
        return self.graph.graphs[sourceKey]

    def getGraphLatestVersions(self, sourceKey):
        assert sourceKey in self.graph.graphVersions
        graphVersions = set(self.graph.graphVersions[sourceKey])
        is_parent = set([])
        for gv in graphVersions:
            if gv.parentIds:
                assert type(gv.parentIds) == list
                for parentId in gv.parentIds:
                    is_parent |= {self.graph.graphVersions[parentId], }
        return list(graphVersions - is_parent)

    def getGraphHistory(self, sourceKey):
        assert sourceKey in self.graph.graphVersions
        parentChild = {}
        for gv in self.graph.graphVersions[sourceKey]:
            if gv.parentIds:
                assert type(gv.parentIds) == list
                for parentId in gv.parentIds:
                    if not parentChild:
                        graphId = gv.graphId
                        parentChild[str(graphId)] = parentId
                    parentChild[str(parentId)] = gv.graphVersionId
        return parentChild

    def getGraphVersion(self, graphVersionId):
        return self.graph.graphVersions[graphVersionId]

    ### STRUCTURES ###
    def createStructure(self, sourceKey, name="null", tags=None):
        if sourceKey not in self.graph.structures:
            structureid = self.graph.gen_id()
            structure = Structure(sourceKey, name, tags)
            structure.structureId = structureid

            self.graph.structures[sourceKey] = structure
            self.graph.structures[structureid] = structure
        else:
            structureid = self.graph.structures[sourceKey].structureId

        return structureid

    def createStructureVersion(self, structureId, attributes, parentIds=None):
        structure = Structure()
        if (structureId in self.graph.structures):
            structure = self.graph.structures[structureId]
        structureVersion = StructureVersion(structure, attributes, parentIds)

        structureversionid = self.graph.gen_id()
        structureVersion.structureVersionId = structureversionid

        if structureVersion.sourceKey in self.graph.structureVersions:
            self.graph.structureVersions[structureVersion.sourceKey].append(structureVersion)
        else:
            self.graph.structureVersions[structureVersion.sourceKey] = [structureVersion, ]
        self.graph.structureVersions[structureversionid] = structureVersion
        return structureversionid

    def getStructure(self, sourceKey):
        return self.graph.structures[sourceKey]

    def getStructureLatestVersions(self, sourceKey):
        assert sourceKey in self.graph.structureVersions
        structureVersions = set(self.graph.structureVersions[sourceKey])
        is_parent = set([])
        for sv in structureVersions:
            if sv.parentIds:
                assert type(sv.parentIds) == list
                for parentId in sv.parentIds:
                    is_parent |= {self.graph.structureVersions[parentId], }
        return list(structureVersions - is_parent)

    def getStructureHistory(self, sourceKey):
        assert sourceKey in self.graph.structureVersions
        parentChild = {}
        for sv in self.graph.structureVersions[sourceKey]:
            if sv.parentIds:
                assert type(sv.parentIds) == list
                for parentId in sv.parentIds:
                    if not parentChild:
                        structureId = sv.structureId
                        parentChild[str(structureId)] = parentId
                    parentChild[str(parentId)] = sv.structureVersionId
        return parentChild

    def getStructureVersion(self, structureVersionId):
        return self.graph.structureVersions[structureVersionId]

    ### LINEAGE EDGES ###
    def createLineageEdge(self, sourceKey, name="null", tags=None):
        if sourceKey not in self.graph.lineageEdges:
            lineageedgeid = self.graph.gen_id()
            lineageEdge = LineageEdge(sourceKey, name, tags)
            lineageEdge.lineageEdgeId = lineageedgeid

            self.graph.lineageEdges[sourceKey] = lineageEdge
            self.graph.lineageEdges[lineageedgeid] = lineageEdge
        else:
            lineageedgeid = self.graph.lineageEdges[sourceKey].lineageEdgeId

        return lineageedgeid

    def createLineageEdgeVersion(self, lineageEdgeId, toRichVersionId, fromRichVersionId, reference=None,
                                 referenceParameters=None, tags=None, structureVersionId=None, parentIds=None):
        lineageEdge = LineageEdge()
        if (lineageEdgeId in self.graph.lineageEdges):
            lineageEdge = self.graph.lineageEdges[lineageEdgeId]
        lineageEdgeVersion = LineageEdgeVersion(lineageEdge, toRichVersionId, fromRichVersionId, reference,
                                                referenceParameters, tags, structureVersionId, parentIds)

        lineageedgeversionid = self.graph.gen_id()
        lineageEdgeVersion.lineageEdgeVersionId = lineageedgeversionid

        if lineageEdgeVersion.sourceKey in self.graph.lineageEdgeVersions:
            self.graph.lineageEdgeVersions[lineageEdgeVersion.sourceKey].append(lineageEdgeVersion)
        else:
            self.graph.lineageEdgeVersions[lineageEdgeVersion.sourceKey] = [lineageEdgeVersion, ]
        self.graph.lineageEdgeVersions[lineageedgeversionid] = lineageEdgeVersion
        return lineageedgeversionid

    def getLineageEdge(self, sourceKey):
        return self.graph.lineageEdges[sourceKey]

    def getLineageEdgeLatestVersions(self, sourceKey):
        assert sourceKey in self.graph.lineageEdgeVersions
        lineageEdgeVersions = set(self.graph.lineageEdgeVersions[sourceKey])
        is_parent = set([])
        for lev in lineageEdgeVersions:
            if lev.parentIds:
                assert type(lev.parentIds) == list
                for parentId in lev.parentIds:
                    is_parent |= {self.graph.lineageEdgeVersions[parentId], }
        return list(lineageEdgeVersions - is_parent)

    def getLineageEdgeHistory(self, sourceKey):
        assert sourceKey in self.graph.lineageEdgeVersions
        parentChild = {}
        for lev in self.graph.lineageEdgeVersions[sourceKey]:
            if lev.parentIds:
                assert type(lev.parentIds) == list
                for parentId in lev.parentIds:
                    if not parentChild:
                        lineageEdgeId = lev.lineageEdgeId
                        parentChild[str(lineageEdgeId)] = parentId
                    parentChild[str(parentId)] = lev.lineageEdgeVersionId
        return parentChild

    def getLineageEdgeVersion(self, lineageEdgeVersionId):
        return self.graph.lineageEdgeVersions[lineageEdgeVersionId]

    ### LINEAGE GRAPHS ###
    def createLineageGraph(self, sourceKey, name="null", tags=None):
        if sourceKey not in self.graph.lineageGraphs:
            lineagegraphid = self.graph.gen_id()
            lineageGraph = LineageGraph(sourceKey, name, tags)
            lineageGraph.lineageGraphId = lineagegraphid

            self.graph.lineageGraphs[sourceKey] = lineageGraph
            self.graph.lineageGraphs[lineagegraphid] = lineageGraph
        else:
            lineagegraphid = self.graph.lineageGraphs[sourceKey].lineageGraphId

        return lineagegraphid

    def createLineageGraphVersion(self, lineageGraphId, lineageEdgeVersionIds, reference=None,
                                  referenceParameters=None, tags=None, structureVersionId=None, parentIds=None):
        lineageGraph = LineageGraph()
        if (lineageGraphId in self.graph.lineageGraphs):
            lineageGraph = self.graph.lineageGraphs[lineageGraphId]
        lineageGraphVersion = LineageGraphVersion(lineageGraph, lineageEdgeVersionIds, reference,
                                                  referenceParameters, tags, structureVersionId, parentIds)

        lineagegraphversionid = self.graph.gen_id()
        lineageGraphVersion.lineageGraphVersionId = lineagegraphversionid

        if lineageGraphVersion.sourceKey in self.graph.lineageGraphVersions:
            self.graph.lineageGraphVersions[lineageGraphVersion.sourceKey].append(lineageGraphVersion)
        else:
            self.graph.lineageGraphVersions[lineageGraphVersion.sourceKey] = [lineageGraphVersion, ]
        self.graph.lineageGraphVersions[lineagegraphversionid] = lineageGraphVersion
        return lineagegraphversionid

    def getLineageGraph(self, sourceKey):
        return self.graph.lineageGraphs[sourceKey]

    def getLineageGraphLatestVersions(self, sourceKey):
        assert sourceKey in self.graph.lineageGraphVersions
        lineageGraphVersions = set(self.graph.lineageGraphVersions[sourceKey])
        is_parent = set([])
        for gev in lineageGraphVersions:
            if gev.parentIds:
                assert type(gev.parentIds) == list
                for parentId in gev.parentIds:
                    is_parent |= {self.graph.lineageGraphVersions[parentId], }
        return list(lineageGraphVersions - is_parent)

    def getLineageGraphHistory(self, sourceKey):
        assert sourceKey in self.graph.lineageGraphVersions
        parentChild = {}
        for gev in self.graph.lineageGraphVersions[sourceKey]:
            if gev.parentIds:
                assert type(gev.parentIds) == list
                for parentId in gev.parentIds:
                    if not parentChild:
                        lineageGraphId = gev.lineageGraphId
                        parentChild[str(lineageGraphId)] = parentId
                    parentChild[str(parentId)] = gev.lineageGraphVersionId
        return parentChild

    def getLineageGraphVersion(self, lineageGraphVersionId):
        return self.graph.lineageGraphVersions[lineageGraphVersionId]

    def commit(self):
        stage = []
        for kee in self.graph.ids:
            if kee in self.graph.nodes:
                serial = self.graph.nodes[kee].to_json()
            elif kee in self.graph.nodeVersions:
                serial = self.graph.nodeVersions[kee].to_json()
            elif kee in self.graph.edges:
                serial = self.graph.edges[kee].to_json()
            elif kee in self.graph.edgeVersions:
                serial = self.graph.edgeVersions[kee].to_json()
            elif kee in self.graph.graphs:
                serial = self.graph.graphs[kee].to_json()
            elif kee in self.graph.graphVersions:
                serial = self.graph.graphVersions[kee].to_json()
            elif kee in self.graph.structures:
                serial = self.graph.structures[kee].to_json()
            elif kee in self.graph.structureVersions:
                serial = self.graph.structureVersions[kee].to_json()
            elif kee in self.graph.lineageEdges:
                serial = self.graph.lineageEdges[kee].to_json()
            elif kee in self.graph.lineageEdgeVersions:
                serial = self.graph.lineageEdgeVersions[kee].to_json()
            elif kee in self.graph.lineageGraphs:
                serial = self.graph.lineageGraphs[kee].to_json()
            else:
                serial = self.graph.lineageGraphVersions[kee].to_json()
            assert serial is not None
            with open(str(kee) + '.json', 'w') as f:
                f.write(serial)
            stage.append(str(kee) + '.json')
        repo = git.Repo.init(os.getcwd())
        repo.index.add(stage)
        repo.index.commit("ground commit")
        tree = repo.tree()
        with open('.jarvis', 'w') as f:
            for obj in tree:
                commithash = self.__run_proc__("git log " + obj.path).replace('\n', ' ').split()[1]
                if obj.path != '.jarvis':
                    f.write(obj.path + " " + commithash + "\n")
        repo.index.add(['.jarvis'])
        repo.index.commit('.jarvis commit')

    def to_class(self, obj):
        if obj['class'] == 'Node':
            n = Node()
            n.sourceKey = obj['sourceKey']
            n.nodeId = obj['nodeId']
            if 'name' in obj:
                n.name = obj['name']
            if 'tags' in obj:
                n.tags = obj['tags']

            self.graph.nodes[n.sourceKey] = n
            self.graph.nodes[n.nodeId] = n
            self.graph.ids |= {n.nodeId, }

        elif obj['class'] == 'NodeVersion':
            nv = NodeVersion()
            nv.sourceKey = obj['sourceKey']
            nv.nodeId = obj['nodeId']
            nv.nodeVersionId = obj['nodeVersionId']
            if 'tags' in obj:
                nv.tags = obj['tags']
            if 'structureVersionId' in obj:
                nv.structureVersionId = obj['structureVersionId']
            if 'reference' in obj:
                nv.reference = obj['reference']
            if 'referenceParameters' in obj:
                nv.referenceParameters = obj['referenceParameters']
            if 'parentIds' in obj:
                nv.parentIds = obj['parentIds']

            if nv.sourceKey in self.graph.nodeVersions:
                self.graph.nodeVersions[nv.sourceKey].append(nv)
            else:
                self.graph.nodeVersions[nv.sourceKey] = [nv, ]
            self.graph.nodeVersions[nv.nodeVersionId] = nv
            self.graph.ids |= {nv.nodeVersionId, }

        elif obj['class'] == 'Edge':
            e = Edge()
            e.sourceKey = obj['sourceKey']
            e.fromNodeId = obj['fromNodeId']
            e.toNodeId = obj['toNodeId']
            e.edgeId = obj['edgeId']
            if 'name' in obj:
                e.name = obj['name']
            if 'tags' in obj:
                e.tags = obj['tags']

            self.graph.edges[e.sourceKey] = e
            self.graph.edges[e.edgeId] = e
            self.graph.ids |= {e.edgeId, }

        elif obj['class'] == 'EdgeVersion':
            ev = EdgeVersion()
            ev.sourceKey = obj['sourceKey']
            ev.edgeId = obj['edgeId']
            ev.toNodeVersionStartId = obj['toNodeVersionStartId']
            ev.fromNodeVersionStartId = obj['fromNodeVersionStartId']
            ev.edgeVersionId = obj['edgeVersionId']
            if 'tags' in obj:
                ev.tags = obj['tags']
            if 'structureVersionId' in obj:
                ev.structureVersionId = obj['structureVersionId']
            if 'reference' in obj:
                ev.reference = obj['reference']
            if 'referenceParameters' in obj:
                ev.referenceParameters = obj['referenceParameters']
            if 'toNodeVersionEndId' in obj:
                ev.toNodeVersionEndId = obj['toNodeVersionEndId']
            if 'fromNodeVersionEndId' in obj:
                ev.fromNodeVersionEndId = obj['fromNodeVersionEndId']
            if 'parentIds' in obj:
                ev.parentIds = obj['parentIds']

            if ev.sourceKey in self.graph.edgeVersions:
                self.graph.edgeVersions[ev.sourceKey].append(ev)
            else:
                self.graph.edgeVersions[ev.sourceKey] = [ev, ]
            self.graph.edgeVersions[ev.edgeVersionId] = ev
            self.graph.ids |= {ev.edgeVersionId, }

        elif obj['class'] == 'Graph':
            g = Graph()
            g.sourceKey = obj['sourceKey']
            g.graphId = obj['graphId']
            if 'name' in obj:
                g.name = obj['name']
            if 'tags' in obj:
                g.tags = obj['tags']

            self.graph.graphs[g.sourceKey] = g
            self.graph.graphs[g.graphId] = g
            self.graph.ids |= {g.graphId, }

        elif obj['class'] == 'GraphVersion':
            gv = GraphVersion()
            gv.sourceKey = obj['sourceKey']
            gv.graphId = obj['graphId']
            gv.edgeVersionIds = obj['edgeVersionIds']
            gv.graphVersionId = obj['graphVersionId']
            if 'tags' in obj:
                gv.tags = obj['tags']
            if 'structureVersionId' in obj:
                gv.structureVersionId = obj['structureVersionId']
            if 'reference' in obj:
                gv.reference = obj['reference']
            if 'referenceParameters' in obj:
                gv.referenceParameters = obj['referenceParameters']
            if 'parentIds' in obj:
                gv.parentIds = obj['parentIds']

            if gv.sourceKey in self.graph.graphVersions:
                self.graph.graphVersions[gv.sourceKey].append(gv)
            else:
                self.graph.graphVersions[gv.sourceKey] = [gv, ]
            self.graph.graphVersions[gv.graphVersionId] = gv
            self.graph.ids |= {gv.graphVersionId, }

        elif obj['class'] == 'Structure':
            s = Structure()
            s.sourceKey = obj['sourceKey']
            s.structureId = obj['structureId']
            if 'name' in obj:
                s.name = obj['name']
            if 'tags' in obj:
                s.tags = obj['tags']

            self.graph.structures[s.sourceKey] = s
            self.graph.structures[s.structureId] = s
            self.graph.ids |= {s.structureId, }

        elif obj['class'] == 'StructureVersion':
            sv = StructureVersion()
            sv.sourceKey = obj['sourceKey']
            sv.structureId = obj['structureId']
            sv.attributes = obj['attributes']
            sv.structureVersionId = obj['structureVersionId']
            if 'parentIds' in obj:
                sv.parentIds = obj['parentIds']

            if sv.sourceKey in self.graph.structureVersions:
                self.graph.structureVersions[sv.sourceKey].append(sv)
            else:
                self.graph.structureVersions[sv.sourceKey] = [sv, ]
            self.graph.structureVersions[sv.structureVersionId] = sv
            self.graph.ids |= {sv.structureVersionId, }

        elif obj['class'] == 'LineageEdge':
            le = LineageEdge()
            le.sourceKey = obj['sourceKey']
            le.lineageEdgeId = obj['lineageEdgeId']
            if 'name' in obj:
                le.name = obj['name']
            if 'tags' in obj:
                le.tags = obj['tags']

            self.graph.lineageEdges[le.sourceKey] = le
            self.graph.lineageEdges[le.lineageEdgeId] = le
            self.graph.ids |= {le.lineageEdgeId, }

        elif obj['class'] == 'LineageEdgeVersion':
            lev = LineageEdgeVersion()
            lev.sourceKey = obj['sourceKey']
            lev.lineageEdgeId = obj['lineageEdgeId']
            lev.toRichVersionId = obj['toRichVersionId']
            lev.fromRichVersionId = obj['fromRichVersionId']
            lev.lineageEdgeVersionId = obj['lineageEdgeVersionId']
            if 'tags' in obj:
                lev.tags = obj['tags']
            if 'structureVersionId' in obj:
                lev.structureVersionId = obj['structureVersionId']
            if 'reference' in obj:
                lev.reference = obj['reference']
            if 'referenceParameters' in obj:
                lev.referenceParameters = obj['referenceParameters']
            if 'parentIds' in obj:
                lev.parentIds = obj['parentIds']

            if lev.sourceKey in self.graph.lineageEdgeVersions:
                self.graph.lineageEdgeVersions[lev.sourceKey].append(lev)
            else:
                self.graph.lineageEdgeVersions[lev.sourceKey] = [lev, ]
            self.graph.lineageEdgeVersions[lev.lineageEdgeVersionId] = lev
            self.graph.ids |= {lev.lineageEdgeVersionId, }

        elif obj['class'] == 'LineageGraph':
            ge = LineageGraph()
            ge.sourceKey = obj['sourceKey']
            ge.lineageGraphId = obj['lineageGraphId']
            if 'name' in obj:
                ge.name = obj['name']
            if 'tags' in obj:
                ge.tags = obj['tags']

            self.graph.lineageGraphs[ge.sourceKey] = ge
            self.graph.lineageGraphs[ge.lineageGraphId] = ge
            self.graph.ids |= {ge.lineageGraphId, }

        elif obj['class'] == 'LineageGraphVersion':
            gev = LineageGraphVersion()
            gev.sourceKey = obj['sourceKey']
            gev.lineageGraphId = obj['lineageGraphId']
            gev.lineageEdgeVersionIds = obj['lineageEdgeVersionIds']
            gev.lineageGraphVersionId = obj['lineageGraphVersionId']
            if 'tags' in obj:
                gev.tags = obj['tags']
            if 'structureVersionId' in obj:
                gev.structureVersionId = obj['structureVersionId']
            if 'reference' in obj:
                gev.reference = obj['reference']
            if 'referenceParameters' in obj:
                gev.referenceParameters = obj['referenceParameters']
            if 'parentIds' in obj:
                gev.parentIds = obj['parentIds']

            if gev.sourceKey in self.graph.lineageGraphVersions:
                self.graph.lineageGraphVersions[gev.sourceKey].append(gev)
            else:
                self.graph.lineageGraphVersions[gev.sourceKey] = [gev, ]
            self.graph.lineageGraphVersions[gev.lineageGraphVersionId] = gev
            self.graph.ids |= {gev.lineageGraphVersionId, }
        else:
            raise NotImplementedError()

    def load(self):
        if self.graph.ids:
            return
        os.chdir('../')

        def is_number(s):
            try:
                float(s)
                return True
            except ValueError:
                return False

        listdir = [x for x in filter(is_number, os.listdir())]

        prevDir = str(len(listdir) - 1)
        os.chdir(prevDir)
        for _, _, filenames in os.walk('.'):
            for filename in filenames:
                filename = filename.split('.')
                if filename[-1] == 'json':
                    filename = '.'.join(filename)
                    with open(filename, 'r') as f:
                        self.to_class(json.loads(f.read()))
        os.chdir('../' + str(int(prevDir) + 1))


class GroundImplementation(GroundAPI):
    def __init__(self, host='localhost', port=9000):
        self.host = host
        self.port = str(port)
        self.url = "http://" + self.host + ":" + self.port


class GroundClient(GroundAPI):
    def __new__(*args, **kwargs):
        if args and args[1].strip().lower() == 'git':
            return GitImplementation(**kwargs)
        elif args and args[1].strip().lower() == 'ground':
            # EXAMPLE CALL: GroundClient('ground', host='localhost', port=9000)
            return GroundImplementation(**kwargs)
        else:
            raise ValueError(
                "Backend not supported. Please choose 'git' or 'ground'")
