/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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

import edu.berkeley.ground.api.models.Edge;
import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.NodeVersion;
import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.plugins.hive.GroundStore.EntityState;
import edu.berkeley.ground.plugins.hive.util.PluginUtil;

/** Helper class for Hive Table related operations. */
public class GroundTable {
    private static final String EDGE = "edge";

    static final private Logger LOG = LoggerFactory.getLogger(GroundTable.class.getName());

    private static final String PARTITION = "partition";

    private static final String TABLE = "table";

    private static final String TABLE_STATE = "TABLE_STATE";

    private GroundReadWrite groundReadWrite = null;
    private GroundPartition groundPartition = null;
    private GroundDatabase groundDatabase = null;

    GroundTable(GroundReadWrite groundReadWrite) {
        this.groundReadWrite = groundReadWrite;
        this.groundPartition = new GroundPartition(groundReadWrite);
        this.groundDatabase = new GroundDatabase(groundReadWrite);
    }

    Node getNode(String tableName, Map<String, Tag> tags) throws GroundException {
        try {
            LOG.debug("Fetching table node: {}", tableName);
            return groundReadWrite.getGroundReadWriteNodeResource().getNode(tableName);
        } catch (GroundException ge1) {
            LOG.debug("Not found - Creating table node: " + tableName);
            Node node = groundReadWrite.getGroundReadWriteNodeResource().createNode(tableName, tags);
            return node;
        }
    }

    NodeVersion createTableNodeVersion(Table table) throws InvalidObjectException, MetaException {
        if (table == null) {
            throw new InvalidObjectException("Database object passed is null");
        }
        try {
            String tableName = table.getTableName();
            List<Long> tableNodeVersionList = GroundReadWrite.getLatestVersions(tableName, "nodes");
            if (tableNodeVersionList != null) {
                LOG.info("table node exists: {}", tableName);
                return groundReadWrite.getGroundReadWriteNodeResource().getNodeVersion(tableNodeVersionList.get(0));
            }
            StructureVersion sv = PluginUtil.getStructureVersion(groundReadWrite, TABLE, EntityState.ACTIVE.name());
            Tag tableTag = new Tag(1L, tableName, PluginUtil.toJson(table), GroundType.STRING);
            String reference = table.getDbName();
            HashMap<String, Tag> tags = new HashMap<>();
            tags.put(tableName, tableTag);
            Node tableNode = this.getNode(tableName, tags);
            Map<String, String> tableParamMap = table.getParameters();
            if (tableParamMap == null) {
                tableParamMap = new HashMap<String, String>();
            }
            Map<String, String> parameters = tableParamMap;
            List<Long> parent = new ArrayList<Long>();
            List<Long> versions = GroundReadWrite.getLatestVersions(tableName, "nodes");
            if (!versions.isEmpty()) {
                parent.add(versions.get(0));
            }
            NodeVersion tableNodeVersion = groundReadWrite.getGroundReadWriteNodeResource()
                    .createNodeVersion(tableNode.getId(), tags, sv.getId(), reference, parameters, tableName);
            // create an edge from database node to tableNode
            Edge edge = groundReadWrite.getGroundReadWriteEdgeResource()
                    .createEdge(table.getDbName() + "-" + table.getTableName(), tags);
            NodeVersion dbNodeVersion = groundReadWrite.getGroundReadWriteNodeResource()
                    .getNodeVersion(GroundReadWrite.getLatestVersions(table.getDbName(), "nodes").get(0));
            groundReadWrite.getGroundReadWriteEdgeResource().createEdgeVersion(edge.getId(), tags, sv.getId(),
                    reference, parameters, edge.getId(), dbNodeVersion.getId(), tableNodeVersion.getId());
            return tableNodeVersion;
        } catch (GroundException e) {
            LOG.error("Failure to create a table node: {}", table.getTableName(), e);
            throw new MetaException(e.getMessage());
        }
    }

    Table getTable(String dbName, String tableName) throws MetaException {
        try {
            List<Long> dbVersions = GroundReadWrite.getLatestVersions(dbName, "nodes");
            if (dbVersions.isEmpty()) {
                throw new MetaException("Database node not found: " + dbName);
            }
            List<Long> adjacentNodeIds = groundReadWrite.getGroundReadWriteNodeResource()
                    .getAdjacentNodes(dbVersions.get(0), dbName + "-" + tableName);
            List<Long> versions = GroundReadWrite.getLatestVersions(tableName, "nodes");
            if (versions.isEmpty()) {
                throw new MetaException("Table node not found: " + tableName);
            }
            NodeVersion latestVersion = groundReadWrite.getGroundReadWriteNodeResource()
                    .getNodeVersion(versions.get(0));
            for (long adjacentNodeId : adjacentNodeIds) {
                if (adjacentNodeId == latestVersion.getId()) {
                    Map<String, Tag> dbTag = latestVersion.getTags();
                    return PluginUtil.fromJson((String) dbTag.get(tableName).getValue(), Table.class);
                }
            }
        } catch (GroundException ex) {
            throw new MetaException(ex.getMessage());
        }
        return null;
    }

    List<String> getTables(String dbName, String pattern) throws MetaException {
        List<String> tables = new ArrayList<String>();
        try {
            List<Long> versions = GroundReadWrite.getLatestVersions(dbName, "nodes");
            if (!versions.isEmpty()) {
                Long dbVersionId = versions.get(0);
                List<Long> tableNodeIds = groundReadWrite.getGroundReadWriteNodeResource().getAdjacentNodes(dbVersionId,
                        pattern);
                for (Long tableNodeId : tableNodeIds) {
                    NodeVersion tableNodeVersion = groundReadWrite.getGroundReadWriteNodeResource()
                            .getNodeVersion(tableNodeId);
                    tables.add(tableNodeVersion.getTags().keySet().iterator().next());
                }
            }
        } catch (GroundException ex) {
            LOG.error("Get tables failed for pattern {}", pattern);
            throw new MetaException(ex.getMessage());
        }
        return tables;
    }

    boolean addPartitions(String dbName, String tableName, List<Partition> parts)
            throws InvalidObjectException, MetaException {
        try {
            Table sourceTable = this.getTable(dbName, tableName);
            NodeVersion tableNodeVersion = createTableNodeVersion(sourceTable);
            long tableNodeVersionId = tableNodeVersion.getId();
            for (Partition part : parts) {
                StringBuilder sb = new StringBuilder();
                NodeVersion nv = groundPartition.createPartition(dbName, tableName, part);
                Edge edge = groundReadWrite.getGroundReadWriteEdgeResource().createEdge(
                        sb.append(dbName).append("-").append(tableName).append("-").append(part).toString(),
                        nv.getTags());
                StructureVersion structureVersion = PluginUtil.getStructureVersion(groundReadWrite, EDGE,
                        EntityState.ACTIVE.name());
                groundReadWrite.getGroundReadWriteEdgeResource().createEdgeVersion(edge.getId(), nv.getTags(),
                        structureVersion.getId(), tableNodeVersion.getReference(), tableNodeVersion.getParameters(),
                        edge.getId(), tableNodeVersionId, nv.getId());
            }
            return true;
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
            List<Long> versions = GroundReadWrite.getLatestVersions(tableName, "nodes");
            if (!versions.isEmpty() && versions.size() > 0) {
                Long tableNodeVersionId = versions.get(0); // table version
                List<Long> tableClosure = groundReadWrite.getTransitiveClosure(tableNodeVersionId);
                long partitionVersionId = GroundReadWrite.getLatestVersions(partName, "nodes").get(0);
                for (Long closureId : tableClosure) {
                    Long version = groundReadWrite.getGroundReadWriteNodeResource().getNodeVersion(closureId).getId();
                    NodeVersion nv = groundReadWrite.getGroundReadWriteNodeResource().getNodeVersion(version);
                    if (nv.getNodeId() == partitionVersionId) {
                        return PluginUtil.fromJson((String) nv.getTags().get(partName).getValue(), Partition.class);
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
        // TODO
        return false;
    }

    List<Partition> getPartitions(String dbName, String tableName, int max)
            throws MetaException, NoSuchObjectException {
        try {
            List<Long> versions = GroundReadWrite.getLatestVersions(tableName, "nodes");
            List<Partition> parts = new ArrayList<Partition>();
            if (!versions.isEmpty()) {
                Long prevVersionId = versions.get(0);
                List<Long> nodeIds = groundReadWrite.getGroundReadWriteNodeResource().getAdjacentNodes(prevVersionId,
                        "");
                for (Long nodeId : nodeIds) {
                    NodeVersion oldNV = groundReadWrite.getGroundReadWriteNodeResource().getNodeVersion(nodeId);
                    parts.add(PluginUtil.fromJson((String) oldNV.getTags().get(oldNV.getNodeId()).getValue(),
                            Partition.class));
                }
            }
            return parts;
        } catch (GroundException ex) {
            LOG.error("Get partitions failed on table {} in database {}", tableName, dbName);
            throw new MetaException(ex.getMessage());
        }

    }

    boolean dropTable(String dbName, String tableName, String state) throws GroundException {
        NodeVersion databaseNodeVersion = this.groundDatabase.getDatabaseNodeVersion(dbName);
        if (databaseNodeVersion == null) {
            LOG.info("Database does not exist: {}", dbName);
            return false;// short circuit database does not exist
        }
        Long tableNodeVersionId = GroundReadWrite.getLatestVersions(tableName, "nodes").get(0);
        NodeVersion tableNodeVersion = groundReadWrite.getGroundReadWriteNodeResource()
                .getNodeVersion(tableNodeVersionId);
        Map<String, Tag> tableTagMap = tableNodeVersion.getTags();
        StructureVersion sv = PluginUtil.getStructureVersion(groundReadWrite, TABLE, state);
        Tag stateTag = new Tag(1L, TABLE_STATE, state, GroundType.STRING);
        tableTagMap.put(TABLE_STATE, stateTag); // update state to deleted
        List<Long> parent = new ArrayList<Long>();
        List<Long> versions = GroundReadWrite.getLatestVersions(dbName, "nodes");
        if (!versions.isEmpty()) {
            LOG.debug("leaves {}", versions.get(0));
            parent.add(versions.get(0));
        }
        LOG.info("database deleted: {}, {}", dbName, databaseNodeVersion.getNodeId());
        this.groundReadWrite.getGroundReadWriteNodeResource().createNodeVersion(1L, tableTagMap, sv.getId(), "",
                new HashMap<String, String>(), tableName);
        return true;
    }

    public List<Partition> getPartitions(String dbName, String tableName, List<String> part_list)
            throws MetaException, NoSuchObjectException {
        List<Partition> partitionList = new ArrayList<Partition>();
        for (String partitionName : part_list) {
            partitionList.add(getPartition(dbName, tableName, partitionName));
        }
        return partitionList;
    }

}