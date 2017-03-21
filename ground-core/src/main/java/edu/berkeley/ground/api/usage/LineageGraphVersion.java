package edu.berkeley.ground.api.usage;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.codehaus.jackson.annotate.JsonCreator;

import java.util.List;
import java.util.Map;

import edu.berkeley.ground.api.models.RichVersion;
import edu.berkeley.ground.api.models.Tag;

public class LineageGraphVersion extends RichVersion {
  // the id of the LineageGraph that contains this version
  private long lineageGraphId;

  // the list of ids of LineageEdgeVersions that are in this LineageGraph
  private List<Long> lineageEdgeVersionIds;

  @JsonCreator
  public LineageGraphVersion(@JsonProperty("id") long id,
                             @JsonProperty("tags") Map<String, Tag> tags,
                             @JsonProperty("structureVersionId") long structureVersionId,
                             @JsonProperty("reference") String reference,
                             @JsonProperty("referenceParameters") Map<String, String> referenceParameters,
                             @JsonProperty("graphId") long lineageGraphId,
                             @JsonProperty("edgeVersionIds") List<Long> lineageEdgeVersionIds) {

    super(id, tags, structureVersionId, reference, referenceParameters);

    this.lineageGraphId = lineageGraphId;
    this.lineageEdgeVersionIds = lineageEdgeVersionIds;
  }

  @JsonProperty
  public long getLineageGraphId() {
    return  this.lineageGraphId;
  }

  @JsonProperty
  public List<Long> getLineageEdgeVersionIds() {
    return this.lineageEdgeVersionIds;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof LineageGraphVersion)) {
      return false;
    }

    LineageGraphVersion otherLineageGraphVersion = (LineageGraphVersion) other;

    return this.lineageGraphId == otherLineageGraphVersion.lineageGraphId &&
        this.lineageEdgeVersionIds.equals(otherLineageGraphVersion.lineageEdgeVersionIds) &&
        super.equals(other);
  }
}
