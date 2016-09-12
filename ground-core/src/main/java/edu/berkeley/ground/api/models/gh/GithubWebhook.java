
package edu.berkeley.ground.api.models.gh;

import com.fasterxml.jackson.annotation.*;

import javax.annotation.Generated;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//https://developer.github.com/v3/activity/events/types/#pushevent

@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("org.jsonschema2pojo")
@JsonPropertyOrder({
  "ref",
  "before",
  "after",
  "created",
  "deleted",
  "forced",
  "base_ref",
  "compare",
  "commits",
  "head_commit",
  "repository",
  "pusher",
  "sender"
})
public class GithubWebhook {

  @JsonProperty("ref")
  private String ref;

  @JsonProperty("before")
  private String before;

  @JsonProperty("after")
  private String after;

  @JsonProperty("created")
  private Boolean created;

  @JsonProperty("deleted")
  private Boolean deleted;

  @JsonProperty("forced")
  private Boolean forced;

  @JsonProperty("base_ref")
  private String baseRef;

  @JsonProperty("compare")
  private String compare;

  @JsonProperty("commits")
  private List<Commit> commits = new ArrayList<Commit>();

  @JsonProperty("head_commit")
  private HeadCommit headCommit;

  @JsonProperty("repository")
  private Repository repository;

  @JsonProperty("pusher")
  private Pusher pusher;

  @JsonProperty("sender")
  private Sender sender;

  @JsonIgnore private Map<String, Object> additionalProperties = new HashMap<String, Object>();

  /** No args constructor for use in serialization */
  public GithubWebhook() {}

  /**
   * @param pusher
   * @param before
   * @param baseRef
   * @param compare
   * @param deleted
   * @param sender
   * @param after
   * @param ref
   * @param created
   * @param repository
   * @param headCommit
   * @param commits
   * @param forced
   */
  public GithubWebhook(
      String ref,
      String before,
      String after,
      Boolean created,
      Boolean deleted,
      Boolean forced,
      String baseRef,
      String compare,
      List<Commit> commits,
      HeadCommit headCommit,
      Repository repository,
      Pusher pusher,
      Sender sender) {
    this.ref = ref;
    this.before = before;
    this.after = after;
    this.created = created;
    this.deleted = deleted;
    this.forced = forced;
    this.baseRef = baseRef;
    this.compare = compare;
    this.commits = commits;
    this.headCommit = headCommit;
    this.repository = repository;
    this.pusher = pusher;
    this.sender = sender;
  }

  /** @return The ref */
  @JsonProperty("ref")
  public String getRef() {
    return ref;
  }

  /** @param ref The ref */
  @JsonProperty("ref")
  public void setRef(String ref) {
    this.ref = ref;
  }

  /** @return The before */
  @JsonProperty("before")
  public String getBefore() {
    return before;
  }

  /** @param before The before */
  @JsonProperty("before")
  public void setBefore(String before) {
    this.before = before;
  }

  /** @return The after */
  @JsonProperty("after")
  public String getAfter() {
    return after;
  }

  /** @param after The after */
  @JsonProperty("after")
  public void setAfter(String after) {
    this.after = after;
  }

  /** @return The created */
  @JsonProperty("created")
  public Boolean getCreated() {
    return created;
  }

  /** @param created The created */
  @JsonProperty("created")
  public void setCreated(Boolean created) {
    this.created = created;
  }

  /** @return The deleted */
  @JsonProperty("deleted")
  public Boolean getDeleted() {
    return deleted;
  }

  /** @param deleted The deleted */
  @JsonProperty("deleted")
  public void setDeleted(Boolean deleted) {
    this.deleted = deleted;
  }

  /** @return The forced */
  @JsonProperty("forced")
  public Boolean getForced() {
    return forced;
  }

  /** @param forced The forced */
  @JsonProperty("forced")
  public void setForced(Boolean forced) {
    this.forced = forced;
  }

  /** @return The baseRef */
  @JsonProperty("base_ref")
  public String getBaseRef() {
    return baseRef;
  }

  /** @param baseRef The base_ref */
  @JsonProperty("base_ref")
  public void setBaseRef(String baseRef) {
    this.baseRef = baseRef;
  }

  /** @return The compare */
  @JsonProperty("compare")
  public String getCompare() {
    return compare;
  }

  /** @param compare The compare */
  @JsonProperty("compare")
  public void setCompare(String compare) {
    this.compare = compare;
  }

  /** @return The commits */
  @JsonProperty("commits")
  public List<Commit> getCommits() {
    return commits;
  }

  /** @param commits The commits */
  @JsonProperty("commits")
  public void setCommits(List<Commit> commits) {
    this.commits = commits;
  }

  /** @return The headCommit */
  @JsonProperty("head_commit")
  public HeadCommit getHeadCommit() {
    return headCommit;
  }

  /** @param headCommit The head_commit */
  @JsonProperty("head_commit")
  public void setHeadCommit(HeadCommit headCommit) {
    this.headCommit = headCommit;
  }

  /** @return The repository */
  @JsonProperty("repository")
  public Repository getRepository() {
    return repository;
  }

  /** @param repository The repository */
  @JsonProperty("repository")
  public void setRepository(Repository repository) {
    this.repository = repository;
  }

  /** @return The pusher */
  @JsonProperty("pusher")
  public Pusher getPusher() {
    return pusher;
  }

  /** @param pusher The pusher */
  @JsonProperty("pusher")
  public void setPusher(Pusher pusher) {
    this.pusher = pusher;
  }

  /** @return The sender */
  @JsonProperty("sender")
  public Sender getSender() {
    return sender;
  }

  /** @param sender The sender */
  @JsonProperty("sender")
  public void setSender(Sender sender) {
    this.sender = sender;
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
