/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 */
package edu.berkeley.ground.common.model.usage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.berkeley.ground.common.model.core.RichVersion;
import edu.berkeley.ground.common.model.version.Tag;
import java.util.Map;

public class LineageEdgeVersion extends RichVersion {

  // the id of the LineageEdge containing this Version
  @JsonProperty("lineageEdgeId")
  private final long lineageEdgeId;

  // the id of the RichVersion that this LineageEdgeVersion originates from
  @JsonProperty("fromRichVersionId")
  private final long fromId;

  // the id of the RichVersion that this LineageEdgeVersion points to
  @JsonProperty("toRichVersionId")
  private final long toId;

  /**
   * Create a lineage edge version.
   *
   * @param id the id of this version
   * @param tags the tags associated with this version
   * @param structureVersionId the id of the StructureVersion associated with this version
   * @param reference an optional external reference
   * @param referenceParameters the access parameters for the reference
   * @param fromId the source rich version id
   * @param toId the destination rich version id
   * @param lineageEdgeId the id of the lineage edge containing this version
   */

  @JsonCreator
  public LineageEdgeVersion(@JsonProperty("id") long id,
                             @JsonProperty("tags") Map<String, Tag> tags,
                             @JsonProperty("structureVersionId") Long structureVersionId,
                             @JsonProperty("reference") String reference,
                             @JsonProperty("referenceParameters") Map<String, String> referenceParameters,
                             @JsonProperty("fromRichVersionId") long fromId,
                             @JsonProperty("toRichVersionId") long toId,
                             @JsonProperty("lineageEdgeId") long lineageEdgeId) {
    super(id, tags, structureVersionId, reference, referenceParameters);

    this.lineageEdgeId = lineageEdgeId;
    this.fromId = fromId;
    this.toId = toId;
  }

  public LineageEdgeVersion(long id, LineageEdgeVersion other) {
    this(id, other, other);
  }

  public LineageEdgeVersion(long id, RichVersion otherRichVersion, LineageEdgeVersion other) {
    super(id, otherRichVersion);

    this.lineageEdgeId = other.lineageEdgeId;
    this.fromId = other.fromId;
    this.toId = other.toId;
  }

  public long getLineageEdgeId() {
    return this.lineageEdgeId;
  }

  public long getFromId() {
    return this.fromId;
  }

  public long getToId() {
    return this.toId;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof LineageEdgeVersion)) {
      return false;
    }

    LineageEdgeVersion otherLineageEdgeVersion = (LineageEdgeVersion) other;

    return this.lineageEdgeId == otherLineageEdgeVersion.lineageEdgeId
             && this.fromId == otherLineageEdgeVersion.fromId
             && this.toId == otherLineageEdgeVersion.toId
             && this.getId() == otherLineageEdgeVersion.getId()
             && super.equals(other);
  }
}
