/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.berkeley.ground.plugins.hive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

import edu.berkeley.ground.api.models.Edge;
import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.NodeVersion;
import edu.berkeley.ground.api.models.Structure;
import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;

public class GroundTable {
    static final private Logger LOG = LoggerFactory.getLogger(GroundTable.class.getName());

    private static final List<String> EMPTY_PARENT_LIST = new ArrayList<String>();

    private GroundReadWrite ground = null;
    private GroundPartition partition = null;

    GroundTable(GroundReadWrite ground) {
        this.ground = ground;
        this.partition = new GroundPartition(ground);
    }

    public Node getNode(String tableName) throws GroundException {
        try {
            LOG.debug("Fetching table node: {}", tableName);
            return ground.getNodeFactory().retrieveFromDatabase(tableName);
        } catch (GroundException ge1) {
            LOG.debug("Not found - Creating table node: " + tableName);

            Node node = ground.getNodeFactory().create(tableName);
            Structure nodeStruct = ground.getStructureFactory().create(node.getName());

            return node;
        }
    }

    public Structure getNodeStructure(String tableName) throws GroundException {
        try {
            Node node = this.getNode(tableName);
            return ground.getStructureFactory().retrieveFromDatabase(tableName);
        } catch (GroundException e) {
            LOG.error("Unable to fetch table node structure");
            throw e;
        }
    }

    public Edge getEdge(NodeVersion nodeVersion) throws GroundException {
        try {
            LOG.debug("Fetching table partition edge: " + nodeVersion.getNodeId());
            return ground.getEdgeFactory().retrieveFromDatabase(nodeVersion.getNodeId());
        } catch (GroundException ge1) {
            LOG.debug("Not found - Creating table partition edge: " + nodeVersion.getNodeId());

            Edge edge = ground.getEdgeFactory().create(nodeVersion.getNodeId());
            Structure edgeStruct = ground.getStructureFactory().create(nodeVersion.getNodeId());
            return edge;
        }
    }

    public Structure getEdgeStructure(NodeVersion nodeVersion) throws GroundException {
        try {
            Edge edge = getEdge(nodeVersion);
            return ground.getStructureFactory().retrieveFromDatabase(nodeVersion.getNodeId());
        } catch (GroundException e) {
            LOG.error("Unable to fetch table partition edge structure");
            throw e;
        }
    }

    Table fromJSON(String json) {
        Gson gson = new Gson();
        return (Table) gson.fromJson(json.replace("\\", ""), Table.class);
    }

    String toJSON(Table table) {
        Gson gson = new Gson();
        return gson.toJson(table);
    }

    public NodeVersion createTable(Table table) throws InvalidObjectException, MetaException {
        if (table == null) {
            throw new InvalidObjectException("Database object passed is null");
        }
        try {
            String tableName = table.getTableName();
            Node tableNode = this.getNode(tableName);
            Structure tableStruct = this.getNodeStructure(tableName);

            Tag tableTag = new Tag("1.0.0", tableName, toJSON(table), GroundType.STRING);

            Map<String, GroundType> structVersionAttribs = new HashMap<>();
            structVersionAttribs.put(tableName, GroundType.STRING);

            StructureVersion sv = ground.getStructureVersionFactory().create(tableStruct.getId(), structVersionAttribs,
                    EMPTY_PARENT_LIST);

            String reference = table.getDbName();
            HashMap<String, Tag> tags = new HashMap<>();
            tags.put(tableName, tableTag);

            Map<String, String> tableParamMap = table.getParameters();
            if (tableParamMap == null) {
                tableParamMap = new HashMap<String, String>();
            }
            Map<String, String> parameters = tableParamMap;

            List<String> parent = new ArrayList<String>();
            List<String> versions = ground.getNodeFactory().getLeaves(tableName);
            if (!versions.isEmpty()) {
                parent.add(versions.get(0));
            }

            NodeVersion tableNodeVersion = ground.getNodeVersionFactory().create(tags, sv.getId(), reference,
                    parameters, tableNode.getId(), parent);
            String tableVersionId = tableNodeVersion.getId();

            return tableNodeVersion;
        } catch (GroundException e) {
            LOG.error("Failure to create a table node: " + table.getTableName());
            throw new MetaException(e.getMessage());
        }
    }

    Table getTable(String dbName, String tableName) throws MetaException {
        try {
            List<String> versions = ground.getNodeFactory().getLeaves(tableName);
            if (versions.isEmpty()) {
                throw new MetaException("Table node not found: " + tableName);
            }

            NodeVersion latestVersion = ground.getNodeVersionFactory().retrieveFromDatabase(versions.get(0));
            Map<String, Tag> dbTag = latestVersion.getTags();

            return this.fromJSON((String) dbTag.get(tableName).getValue());
        } catch (GroundException ex) {
            throw new MetaException(ex.getMessage());
        }
    }

    List<String> getTables(String dbName, String pattern) throws MetaException {
        List<String> tables = new ArrayList<String>();
        try {
            List<String> versions = ground.getNodeFactory().getLeaves(dbName);

            if (!versions.isEmpty()) {
                String metaVersionId = versions.get(0);
                List<String> tableNodeIds = ground.getNodeVersionFactory().getAdjacentNodes(metaVersionId, pattern);
                for (String tableNodeId : tableNodeIds) {
                    NodeVersion tableNodeVersion = ground.getNodeVersionFactory().retrieveFromDatabase(tableNodeId);
                    // create an edge for a dbname only if was not created
                    // earlier
                    Edge edge = ground.getEdgeFactory().retrieveFromDatabase(tableNodeVersion.getNodeId());
                    tables.add(edge.getName().split("Nodes.")[1]);
                }
            }
        } catch (GroundException ex) {
            LOG.error("Get tables failed for pattern {}", pattern);
            throw new MetaException(ex.getMessage());
        }
        return tables;
    }

    NodeVersion addPartitions(String dbName, String tableName, List<Partition> parts)
            throws InvalidObjectException, MetaException {
        try {
            Table prevTable = this.getTable(dbName, tableName);

            List<String> versions = ground.getNodeFactory().getLeaves(tableName);

            NodeVersion tableNodeVersion = this.createTable(prevTable);
            String tableNodeVersionId = tableNodeVersion.getId();

            if (!versions.isEmpty() && versions.size() > 0) {
                String prevVersionId = versions.get(0);
                List<String> nodeIds = ground.getNodeVersionFactory().getAdjacentNodes(prevVersionId, "");
                for (String nodeId : nodeIds) {
                    NodeVersion oldNV = ground.getNodeVersionFactory().retrieveFromDatabase(nodeId);
                    Edge edge = this.getEdge(oldNV);
                    Structure structure = this.getEdgeStructure(oldNV);

                    Map<String, GroundType> structVersionAttribs = new HashMap<>();
                    for (String key : oldNV.getTags().keySet()) {
                        structVersionAttribs.put(key, GroundType.STRING);
                    }

                    // create an edge version for a dbname
                    StructureVersion sv = ground.getStructureVersionFactory().create(structure.getId(),
                            structVersionAttribs, new ArrayList<>());
                    ground.getEdgeVersionFactory().create(oldNV.getTags(), sv.getId(), oldNV.getReference(),
                            oldNV.getParameters(), edge.getId(), tableNodeVersionId, oldNV.getId(),
                            new ArrayList<String>());
                }
            }

            for (Partition part : parts) {
                NodeVersion nv = partition.createPartition(dbName, tableName, part);

                Edge edge = this.getEdge(nv);
                Structure structure = this.getEdgeStructure(nv);
                Map<String, GroundType> structVersionAttribs = new HashMap<>();
                for (String key : nv.getTags().keySet()) {
                    structVersionAttribs.put(key, GroundType.STRING);
                }
                StructureVersion sv = ground.getStructureVersionFactory().create(structure.getId(),
                        structVersionAttribs, new ArrayList<>());

                ground.getEdgeVersionFactory().create(nv.getTags(), sv.getId(), tableNodeVersion.getReference(),
                        tableNodeVersion.getParameters(), edge.getId(), tableNodeVersionId, nv.getId(),
                        EMPTY_PARENT_LIST);
            }

            return tableNodeVersion;
        } catch (InvalidObjectException | MetaException ex) {
            LOG.error("Unable to create partition to table {} database {}", tableName, dbName);
            throw ex;
        } catch (GroundException ex) {
            LOG.error("Unable to create partition to table {} database {}", tableName, dbName);
            throw new MetaException(ex.getMessage());
        }
    }

    Partition getPartition(String dbName, String tableName, String partName)
            throws MetaException, NoSuchObjectException {
        try {
            List<String> versions = ground.getNodeFactory().getLeaves(tableName);
            if (!versions.isEmpty() && versions.size() > 0) {
                String tableNodeVersionId = versions.get(0);
                List<String> partNodeVersionIds = ground.getNodeVersionFactory()
                        .getTransitiveClosure(tableNodeVersionId);

                String partNodeName = "Nodes." + partName;
                for (String partNodeVersionId : partNodeVersionIds) {
                    String version = ground.getNodeFactory().getLeaves(partNodeVersionId).get(0);
                    NodeVersion nv = ground.getNodeVersionFactory().retrieveFromDatabase(version);
                    if (nv.getNodeId().equals(partNodeName)) {
                        return this.partition.fromJSON((String) nv.getTags().get(partName).getValue());
                    }
                }
            }
            throw new NoSuchObjectException(
                    "Unable to find partition " + partName + " for table " + tableName + " in database " + dbName);
        } catch (GroundException ex) {
            LOG.error("Unable to find partition {} for table {} in database {}", partName, tableName, dbName);
            throw new MetaException(ex.getMessage());
        }
    }

    boolean dropPartition(String dbName, String tableName, List<String> part_vals)
            throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        // TODO Auto-generated method stub
        return false;
    }

    List<Partition> getPartitions(String dbName, String tableName, int max)
            throws MetaException, NoSuchObjectException {
        try {
            List<String> versions = ground.getNodeFactory().getLeaves(tableName);
            List<Partition> parts = new ArrayList<Partition>();
            if (!versions.isEmpty()) {
                String prevVersionId = versions.get(0);
                List<String> nodeIds = ground.getNodeVersionFactory().getAdjacentNodes(prevVersionId, "");
                for (String nodeId : nodeIds) {
                    NodeVersion oldNV = ground.getNodeVersionFactory().retrieveFromDatabase(nodeId);
                    parts.add(this.partition
                            .fromJSON((String) oldNV.getTags().get(oldNV.getNodeId().split("Nodes.")[1]).getValue()));
                }
            }
            return parts;
        } catch (GroundException ex) {
            LOG.error("Get partitions failed on table {} in database {}", tableName, dbName);
            throw new MetaException(ex.getMessage());
        }

    }

}