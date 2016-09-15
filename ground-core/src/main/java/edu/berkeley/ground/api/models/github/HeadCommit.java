
package edu.berkeley.ground.api.models.github;

import com.fasterxml.jackson.annotation.*;

import javax.annotation.Generated;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("org.jsonschema2pojo")
@JsonPropertyOrder({
  "id",
  "tree_id",
  "distinct",
  "message",
  "timestamp",
  "url",
  "author",
  "committer",
  "added",
  "removed",
  "modified"
})
public class HeadCommit {

  @JsonProperty("id")
  private String id;

  @JsonProperty("tree_id")
  private String treeId;

  @JsonProperty("distinct")
  private Boolean distinct;

  @JsonProperty("message")
  private String message;

  @JsonProperty("timestamp")
  private String timestamp;

  @JsonProperty("url")
  private String url;

  @JsonProperty("author")
  private Author author;

  @JsonProperty("committer")
  private Committer committer;

  @JsonProperty("added")
  private List<Object> added = new ArrayList<Object>();

  @JsonProperty("removed")
  private List<Object> removed = new ArrayList<Object>();

  @JsonProperty("modified")
  private List<String> modified = new ArrayList<String>();

  @JsonIgnore private Map<String, Object> additionalProperties = new HashMap<String, Object>();

  /** No args constructor for use in serialization */
  public HeadCommit() {}

  /**
   * @param timestamp
   * @param message
   * @param treeId
   * @param id
   * @param author
   * @param added
   * @param removed
   * @param committer
   * @param url
   * @param modified
   * @param distinct
   */
  public HeadCommit(
      String id,
      String treeId,
      Boolean distinct,
      String message,
      String timestamp,
      String url,
      Author author,
      Committer committer,
      List<Object> added,
      List<Object> removed,
      List<String> modified) {
    this.id = id;
    this.treeId = treeId;
    this.distinct = distinct;
    this.message = message;
    this.timestamp = timestamp;
    this.url = url;
    this.author = author;
    this.committer = committer;
    this.added = added;
    this.removed = removed;
    this.modified = modified;
  }

  /** @return The id */
  @JsonProperty("id")
  public String getId() {
    return id;
  }

  /** @param id The id */
  @JsonProperty("id")
  public void setId(String id) {
    this.id = id;
  }

  /** @return The treeId */
  @JsonProperty("tree_id")
  public String getTreeId() {
    return treeId;
  }

  /** @param treeId The tree_id */
  @JsonProperty("tree_id")
  public void setTreeId(String treeId) {
    this.treeId = treeId;
  }

  /** @return The distinct */
  @JsonProperty("distinct")
  public Boolean getDistinct() {
    return distinct;
  }

  /** @param distinct The distinct */
  @JsonProperty("distinct")
  public void setDistinct(Boolean distinct) {
    this.distinct = distinct;
  }

  /** @return The message */
  @JsonProperty("message")
  public String getMessage() {
    return message;
  }

  /** @param message The message */
  @JsonProperty("message")
  public void setMessage(String message) {
    this.message = message;
  }

  /** @return The timestamp */
  @JsonProperty("timestamp")
  public String getTimestamp() {
    return timestamp;
  }

  /** @param timestamp The timestamp */
  @JsonProperty("timestamp")
  public void setTimestamp(String timestamp) {
    this.timestamp = timestamp;
  }

  /** @return The url */
  @JsonProperty("url")
  public String getUrl() {
    return url;
  }

  /** @param url The url */
  @JsonProperty("url")
  public void setUrl(String url) {
    this.url = url;
  }

  /** @return The author */
  @JsonProperty("author")
  public Author getAuthor() {
    return author;
  }

  /** @param author The author */
  @JsonProperty("author")
  public void setAuthor(Author author) {
    this.author = author;
  }

  /** @return The committer */
  @JsonProperty("committer")
  public Committer getCommitter() {
    return committer;
  }

  /** @param committer The committer */
  @JsonProperty("committer")
  public void setCommitter(Committer committer) {
    this.committer = committer;
  }

  /** @return The added */
  @JsonProperty("added")
  public List<Object> getAdded() {
    return added;
  }

  /** @param added The added */
  @JsonProperty("added")
  public void setAdded(List<Object> added) {
    this.added = added;
  }

  /** @return The removed */
  @JsonProperty("removed")
  public List<Object> getRemoved() {
    return removed;
  }

  /** @param removed The removed */
  @JsonProperty("removed")
  public void setRemoved(List<Object> removed) {
    this.removed = removed;
  }

  /** @return The modified */
  @JsonProperty("modified")
  public List<String> getModified() {
    return modified;
  }

  /** @param modified The modified */
  @JsonProperty("modified")
  public void setModified(List<String> modified) {
    this.modified = modified;
  }

  @JsonAnyGetter
  public Map<String, Object> getAdditionalProperties() {
    return this.additionalProperties;
  }

  @JsonAnySetter
  public void setAdditionalProperty(String name, Object value) {
    this.additionalProperties.put(name, value);
  }
}
