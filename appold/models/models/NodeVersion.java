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

package models.models;




import java.util.Map;

public class NodeVersion extends RichVersion {
  // the id of the Node containing this Version
  private final long nodeId;

  /**
   * Create a new node version.
   *
   * @param id the id of the version
   * @param tags the tags associated with the version
   * @param structureVersionId the id of the StructureVersion associated with this version
   * @param reference an optional external reference
   * @param referenceParameters the parameters associated with the reference
   * @param nodeId the id of the node containing this version
   */
  
  public NodeVersion(
      ("id") long id,
      ("tags") Map<String, Tag> tags,
      ("structureVersionId") long structureVersionId,
      ("reference") String reference,
      ("referenceParameters") Map<String, String> referenceParameters,
      ("nodeId") long nodeId) {

    super(id, tags, structureVersionId, reference, referenceParameters);

    this.nodeId = nodeId;
  }

  
  public long getNodeId() {
    return this.nodeId;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof NodeVersion)) {
      return false;
    }

    NodeVersion otherNodeVersion = (NodeVersion) other;

    return this.nodeId == otherNodeVersion.nodeId
        && super.equals(other);
  }
}
