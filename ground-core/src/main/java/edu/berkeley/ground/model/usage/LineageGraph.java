package edu.berkeley.ground.model.usage;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.codehaus.jackson.annotate.JsonCreator;

import java.util.Map;

import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.Item;

public class LineageGraph extends Item<LineageGraphVersion> {
  // the name of this graph
  private String name;

  // the source key for this Node
  private String sourceKey;

  @JsonCreator
  public LineageGraph(@JsonProperty("id") long id,
                      @JsonProperty("name") String name,
                      @JsonProperty("source_key") String sourceKey,
                      @JsonProperty("tags") Map<String, Tag> tags) {
    super(id, tags);

    this.name = name;
    this.sourceKey = sourceKey;
  }

  @JsonProperty
  public String getName() {
    return this.name;
  }

  @JsonProperty
  public String getSourceKey() {
    return this.sourceKey;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof LineageGraph)) {
      return false;
    }

    LineageGraph otherLineageGraph = (LineageGraph) other;
    return this.name.equals(otherLineageGraph.name) &&
        this.getId() == otherLineageGraph.getId() &&
        this.sourceKey.equals(otherLineageGraph.sourceKey);
  }
}
