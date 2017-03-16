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

import edu.berkeley.ground.api.models.Edge;
import edu.berkeley.ground.api.models.EdgeVersion;
import edu.berkeley.ground.api.models.Node;
import edu.berkeley.ground.api.models.NodeVersion;
import edu.berkeley.ground.api.models.Structure;
import edu.berkeley.ground.api.models.StructureVersion;
import edu.berkeley.ground.api.models.Tag;
import edu.berkeley.ground.api.versions.GroundType;
import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.plugins.hive.GroundStore.EntityState;
import edu.berkeley.ground.plugins.hive.util.PluginUtil;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GroundDatabase {
    private static final String DATABASE = "database";

    static final private Logger LOG = LoggerFactory.getLogger(GroundDatabase.class.getName());

    static final String DATABASE_NODE = "_DATABASE";

    static final String DATABASE_TABLE_EDGE = "_DATABASE_TABLE";

    private static final String DB_STATE = "DATABASE_STATE";

    private GroundReadWrite groundReadWrite = null;
    private GroundTable groundTable = null;

    GroundDatabase(GroundReadWrite groundReadWrite) {
        this.groundReadWrite = groundReadWrite;
        this.groundTable = new GroundTable(groundReadWrite);
    }

    Node getNode(String dbName) throws GroundException {
        LOG.debug("Fetching database node: " + dbName);
        return this.groundReadWrite.getNode(dbName);
    }

    Structure getNodeStructure(String dbName) throws GroundException {
        return this.groundReadWrite.getStructure(dbName);
    }

    Edge getEdge(EdgeVersion edgeVersion) throws GroundException {
        //TODO
        return null;
    }

    Structure getEdgeStructure(EdgeVersion edgeVersion) throws GroundException {
        try {
            LOG.debug("Fetching database table edge structure: " + edgeVersion.getEdgeId());
            Edge edge = this.getEdge(edgeVersion);
            return this.groundReadWrite.getStructure(edge.getName());
        } catch (GroundException e) {
            LOG.debug("Not found - database table edge structure: " + edgeVersion.getEdgeId());
            throw new GroundException(e);
        }
    }

    Database getDatabase(String dbName) throws GroundException {
        NodeVersion latestVersion = getDatabaseNodeVersion(dbName);
        Map<String, Tag> dbTag = latestVersion.getTags();
        return PluginUtil.fromJSON((String) dbTag.get(dbName).getValue(), Database.class);
    }

    NodeVersion getDatabaseNodeVersion(String dbName) throws GroundException {
        List<Long> versions = this.groundReadWrite.getLatestVersions(dbName);
        if (versions.isEmpty()) {
            throw new GroundException("Database node not found: " + dbName);
        }
        NodeVersion latestVersion = this.groundReadWrite.getNodeVersion(versions.get(0));
        return latestVersion;
    }

    NodeVersion createDatabaseNodeVersion(Database db, String state) throws InvalidObjectException, MetaException {
        if (db == null) {
            throw new InvalidObjectException("Database object passed is null");
        }
        try {
            StructureVersion sv = getDatabaseStructureVersion(state);
            // create a new tag for new database node version entry
            String dbName = db.getName();
            Node dbNode = this.getNode(dbName);
            String reference = db.getLocationUri();
            Map<String, Tag> tags = new HashMap<>();
            Tag stateTag = new Tag(1L, dbName + DB_STATE, state, GroundType.STRING);
            tags.put(DB_STATE, stateTag);
            Tag dbTag = new Tag(1L, dbName, PluginUtil.toJSON(db), GroundType.STRING);
            tags.put(dbName, dbTag);
            tags.put(DB_STATE, stateTag);
            Map<String, String> referenceParameterMap = db.getParameters();
            if (referenceParameterMap == null) {
                referenceParameterMap = new HashMap<String, String>();
            }
            List<Long> parent = new ArrayList<Long>();
            List<Long> versions = this.groundReadWrite.getLatestVersions(dbName);
            if (!versions.isEmpty()) {
                LOG.debug("leaves {}", versions.get(0));
                parent.add(versions.get(0));
            }
            return this.groundReadWrite.createNodeVersion(1L, tags, sv.getId(), reference, referenceParameterMap,
                    dbNode.getId());
        } catch (GroundException e) {
            LOG.error("Failure to create a database node: {}", e);
            throw new MetaException(e.getMessage());
        }
    }

    private StructureVersion getDatabaseStructureVersion(String state) throws GroundException {
        Structure dbStruct = this.groundReadWrite.getStructure(DATABASE);
        StructureVersion sv;
        if (dbStruct == null) { // create a new structure version
            dbStruct = this.groundReadWrite.createStructure(DATABASE);
            LOG.debug("Node and Structure {}", dbStruct.getId());
            Map<String, GroundType> structVersionAttribs = new HashMap<>();
            structVersionAttribs.put(state, GroundType.STRING);
            sv = this.groundReadWrite.createStructureVersion(dbStruct.getId(), dbStruct.getId(), structVersionAttribs);
        } else {
            sv = this.groundReadWrite.getStructureVersion(dbStruct.getId());
        }
        return sv;
    }

    // Table related functions
    NodeVersion createTableComponents(Table table) throws InvalidObjectException, MetaException {
        return null;
    }

    NodeVersion dropTableNodeVersion(String dbName, String tableName)
            throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
        return null;
    }

    Table getTable(String dbName, String tableName) throws MetaException {
        return groundTable.getTable(dbName, tableName);
    }

    List<String> getTables(String dbName, String pattern) throws MetaException {
        return groundTable.getTables(dbName, pattern);
    }

    NodeVersion addPartitions(String dbName, String tableName, List<Partition> parts)
            throws InvalidObjectException, MetaException {
        return null;
    }

    Partition getPartition(String dbName, String tableName, String partName)
            throws NoSuchObjectException, MetaException {
        try {
            return groundTable.getPartition(dbName, tableName, partName);
        } catch (MetaException | NoSuchObjectException ex) {
            LOG.error("Unable to ger partition {} fro table {} database {}", partName, tableName, dbName);
            throw ex;
        }
    }

    List<Partition> getPartitions(String dbName, String tableName, int max)
            throws MetaException, NoSuchObjectException {
        try {
            return groundTable.getPartitions(dbName, tableName, max);
        } catch (MetaException | NoSuchObjectException ex) {
            throw ex;
        }
    }

    NodeVersion dropDatabase(String dbName, String state) throws GroundException {
        Node dbNode = getNode(dbName);
        NodeVersion databaseNodeVersion = getDatabaseNodeVersion(dbName);
        Map<String, Tag> dbTagMap = databaseNodeVersion.getTags();
        if (dbTagMap == null) {
            LOG.info("node version getTags failed");
            return null;
        }
        StructureVersion sv = getDatabaseStructureVersion(state);
        Tag stateTag = new Tag(1L, DB_STATE, state, GroundType.STRING);
        dbTagMap.put(DB_STATE, stateTag); //update state to deleted
        List<Long> parent = new ArrayList<Long>();
        List<Long> versions = this.groundReadWrite.getLatestVersions(dbName);
        if (!versions.isEmpty()) {
            LOG.debug("leaves {}", versions.get(0));
            parent.add(versions.get(0));
        }
        LOG.info("database deleted: {}, {}", dbName, databaseNodeVersion.getNodeId());
        return this.groundReadWrite.createNodeVersion(1L, dbTagMap, sv.getId(), "", new HashMap<String, String>(),
                dbNode.getId());
    }

    public List<String> getDatabases(String pattern) {
        // TODO Auto-generated method stub
        return null;
    }
}
