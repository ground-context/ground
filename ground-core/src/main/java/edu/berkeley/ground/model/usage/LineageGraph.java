package edu.berkeley.ground.model.usage;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.codehaus.jackson.annotate.JsonCreator;

import java.util.Map;

import edu.berkeley.ground.model.models.Tag;
import edu.berkeley.ground.model.versions.Item;

public class LineageGraph extends Item<LineageGraphVersion> {
  // the name of this graph
  private String name;

  @JsonCreator
  public LineageGraph(@JsonProperty("id") long id,
                      @JsonProperty("name") String name,
                      @JsonProperty("tags") Map<String, Tag> tags) {
    super(id, tags);

    this.name = name;
  }

  @JsonProperty
  public String getName() {
    return this.name;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof LineageGraph)) {
      return false;
    }

    LineageGraph otherGraph = (LineageGraph) other;
    return this.name.equals(otherGraph.name) && this.getId() == otherGraph.getId();
  }
}
