/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.plugins.hive;

import edu.berkeley.ground.exceptions.GroundException;
import edu.berkeley.ground.model.models.Edge;
import edu.berkeley.ground.model.models.Node;
import edu.berkeley.ground.model.models.NodeVersion;
import edu.berkeley.ground.model.models.Structure;
import edu.berkeley.ground.model.models.StructureVersion;
import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.GroundType;
import edu.berkeley.ground.plugins.hive.util.JsonUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hive.common.util.HiveStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroundPartition {

  private static final Logger LOG = LoggerFactory.getLogger(GroundTable.class.getName());

  private GroundReadWrite groundReadWrite = null;

  public GroundPartition(GroundReadWrite ground) {
    groundReadWrite = ground;
  }

  /**
   * Retrieve the partition node.
   *
   * @param partitionName the name of the partition
   * @return the partition node
   * @throws GroundException an error while retrieving the node
   */
  public Node getNode(String partitionName) throws GroundException {
    try {
      LOG.debug("Fetching partition node: " + partitionName);
      return groundReadWrite.getNodeFactory().retrieveFromDatabase(partitionName);
    } catch (GroundException ge1) {
      LOG.debug("Not found - Creating partition node: {}", partitionName);

      Node node = groundReadWrite.getNodeFactory().create(partitionName, null, new HashMap<>());
      Structure nodeStruct = groundReadWrite.getStructureFactory().create(node.getName(), null,
          new HashMap<>());

      return node;
    }
  }

  /**
   * Retrieve the structure for partitions.
   *
   * @param partitionName the name of the partition
   * @return the structure for partitions
   * @throws GroundException an error while retrieving the structure
   */
  public Structure getNodeStructure(String partitionName) throws GroundException {
    try {
      Node node = this.getNode(partitionName);
      return groundReadWrite.getStructureFactory().retrieveFromDatabase(partitionName);
    } catch (GroundException e) {
      LOG.error("Unable to fetch partition node structure");
      throw e;
    }
  }

  /**
   * Retrieve a partition's edge.
   *
   * @param partitionName the name of the partition
   * @return the partition's edge
   * @throws GroundException an error retrieving the edge
   */
  public Edge getEdge(String partitionName) throws GroundException {
    try {
      LOG.debug("Fetching table partition edge: " + partitionName);
      return groundReadWrite.getEdgeFactory().retrieveFromDatabase(partitionName);
    } catch (GroundException ge1) {
      LOG.debug("Not found - Creating table partition edge: {}", partitionName);

      Edge edge = groundReadWrite.getEdgeFactory().create(partitionName, null, 1, 2,
          new HashMap<>());
      Structure edgeStruct = groundReadWrite.getStructureFactory().create(partitionName, null,
          new HashMap<>());
      return edge;
    }
  }

  /**
   * Create a new partition.
   *
   * @param dbName the name of the database
   * @param tableName the name of the table
   * @param part the partition's data
   * @return the node version corresponding the partition
   * @throws InvalidObjectException an invalid partition
   * @throws MetaException an exception while creating the partition
   */
  public NodeVersion createPartition(String dbName, String tableName, Partition part)
      throws InvalidObjectException, MetaException {
    try {
      ObjectPair<String, String> objectPair = new ObjectPair<>(HiveStringUtils
          .normalizeIdentifier(dbName), HiveStringUtils.normalizeIdentifier(tableName));
      String partId = objectPair.toString();
      for (String value : part.getValues()) {
        partId += ":" + value;
      }

      Tag partTag = new Tag(0, partId, JsonUtil.toJson(part), GroundType.STRING);

      Node node = this.getNode(partId);
      long nodeId = node.getId();
      Structure partStruct = this.getNodeStructure(partId);
      Map<String, GroundType> structVersionAttribs = new HashMap<>();
      structVersionAttribs.put(partId, GroundType.STRING);
      StructureVersion sv = groundReadWrite.getStructureVersionFactory().create(partStruct.getId(),
          structVersionAttribs, new ArrayList<>());

      String reference = part.getSd().getLocation();
      HashMap<String, Tag> tags = new HashMap<>();
      tags.put(partId, partTag);

      long versionId = sv.getId();
      List<Long> parentId = new ArrayList<>();

      Map<String, String> parameters = part.getParameters();

      return groundReadWrite.getNodeVersionFactory().create(tags, versionId, reference, parameters,
          nodeId, parentId);
    } catch (GroundException e) {
      throw new MetaException("Unable to create partition " + e.getMessage());
    }
  }
}
