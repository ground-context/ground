
package edu.berkeley.ground.api.models.github;

import com.fasterxml.jackson.annotation.*;

import javax.annotation.Generated;
import java.util.HashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("org.jsonschema2pojo")
@JsonPropertyOrder({
  "login",
  "id",
  "avatar_url",
  "gravatar_id",
  "url",
  "html_url",
  "followers_url",
  "following_url",
  "gists_url",
  "starred_url",
  "subscriptions_url",
  "organizations_url",
  "repos_url",
  "events_url",
  "received_events_url",
  "type",
  "site_admin"
})
public class Sender {

  @JsonProperty("login")
  private String login;

  @JsonProperty("id")
  private Integer id;

  @JsonProperty("avatar_url")
  private String avatarUrl;

  @JsonProperty("gravatar_id")
  private String gravatarId;

  @JsonProperty("url")
  private String url;

  @JsonProperty("html_url")
  private String htmlUrl;

  @JsonProperty("followers_url")
  private String followersUrl;

  @JsonProperty("following_url")
  private String followingUrl;

  @JsonProperty("gists_url")
  private String gistsUrl;

  @JsonProperty("starred_url")
  private String starredUrl;

  @JsonProperty("subscriptions_url")
  private String subscriptionsUrl;

  @JsonProperty("organizations_url")
  private String organizationsUrl;

  @JsonProperty("repos_url")
  private String reposUrl;

  @JsonProperty("events_url")
  private String eventsUrl;

  @JsonProperty("received_events_url")
  private String receivedEventsUrl;

  @JsonProperty("type")
  private String type;

  @JsonProperty("site_admin")
  private Boolean siteAdmin;

  @JsonIgnore private Map<String, Object> additionalProperties = new HashMap<String, Object>();

  /** No args constructor for use in serialization */
  public Sender() {}

  /**
   * @param eventsUrl
   * @param siteAdmin
   * @param gistsUrl
   * @param type
   * @param gravatarId
   * @param url
   * @param subscriptionsUrl
   * @param id
   * @param followersUrl
   * @param reposUrl
   * @param htmlUrl
   * @param receivedEventsUrl
   * @param avatarUrl
   * @param followingUrl
   * @param login
   * @param organizationsUrl
   * @param starredUrl
   */
  public Sender(
      String login,
      Integer id,
      String avatarUrl,
      String gravatarId,
      String url,
      String htmlUrl,
      String followersUrl,
      String followingUrl,
      String gistsUrl,
      String starredUrl,
      String subscriptionsUrl,
      String organizationsUrl,
      String reposUrl,
      String eventsUrl,
      String receivedEventsUrl,
      String type,
      Boolean siteAdmin) {
    this.login = login;
    this.id = id;
    this.avatarUrl = avatarUrl;
    this.gravatarId = gravatarId;
    this.url = url;
    this.htmlUrl = htmlUrl;
    this.followersUrl = followersUrl;
    this.followingUrl = followingUrl;
    this.gistsUrl = gistsUrl;
    this.starredUrl = starredUrl;
    this.subscriptionsUrl = subscriptionsUrl;
    this.organizationsUrl = organizationsUrl;
    this.reposUrl = reposUrl;
    this.eventsUrl = eventsUrl;
    this.receivedEventsUrl = receivedEventsUrl;
    this.type = type;
    this.siteAdmin = siteAdmin;
  }

  /** @return The login */
  @JsonProperty("login")
  public String getLogin() {
    return login;
  }

  /** @param login The login */
  @JsonProperty("login")
  public void setLogin(String login) {
    this.login = login;
  }

  /** @return The id */
  @JsonProperty("id")
  public Integer getId() {
    return id;
  }

  /** @param id The id */
  @JsonProperty("id")
  public void setId(Integer id) {
    this.id = id;
  }

  /** @return The avatarUrl */
  @JsonProperty("avatar_url")
  public String getAvatarUrl() {
    return avatarUrl;
  }

  /** @param avatarUrl The avatar_url */
  @JsonProperty("avatar_url")
  public void setAvatarUrl(String avatarUrl) {
    this.avatarUrl = avatarUrl;
  }

  /** @return The gravatarId */
  @JsonProperty("gravatar_id")
  public String getGravatarId() {
    return gravatarId;
  }

  /** @param gravatarId The gravatar_id */
  @JsonProperty("gravatar_id")
  public void setGravatarId(String gravatarId) {
    this.gravatarId = gravatarId;
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

  /** @return The htmlUrl */
  @JsonProperty("html_url")
  public String getHtmlUrl() {
    return htmlUrl;
  }

  /** @param htmlUrl The html_url */
  @JsonProperty("html_url")
  public void setHtmlUrl(String htmlUrl) {
    this.htmlUrl = htmlUrl;
  }

  /** @return The followersUrl */
  @JsonProperty("followers_url")
  public String getFollowersUrl() {
    return followersUrl;
  }

  /** @param followersUrl The followers_url */
  @JsonProperty("followers_url")
  public void setFollowersUrl(String followersUrl) {
    this.followersUrl = followersUrl;
  }

  /** @return The followingUrl */
  @JsonProperty("following_url")
  public String getFollowingUrl() {
    return followingUrl;
  }

  /** @param followingUrl The following_url */
  @JsonProperty("following_url")
  public void setFollowingUrl(String followingUrl) {
    this.followingUrl = followingUrl;
  }

  /** @return The gistsUrl */
  @JsonProperty("gists_url")
  public String getGistsUrl() {
    return gistsUrl;
  }

  /** @param gistsUrl The gists_url */
  @JsonProperty("gists_url")
  public void setGistsUrl(String gistsUrl) {
    this.gistsUrl = gistsUrl;
  }

  /** @return The starredUrl */
  @JsonProperty("starred_url")
  public String getStarredUrl() {
    return starredUrl;
  }

  /** @param starredUrl The starred_url */
  @JsonProperty("starred_url")
  public void setStarredUrl(String starredUrl) {
    this.starredUrl = starredUrl;
  }

  /** @return The subscriptionsUrl */
  @JsonProperty("subscriptions_url")
  public String getSubscriptionsUrl() {
    return subscriptionsUrl;
  }

  /** @param subscriptionsUrl The subscriptions_url */
  @JsonProperty("subscriptions_url")
  public void setSubscriptionsUrl(String subscriptionsUrl) {
    this.subscriptionsUrl = subscriptionsUrl;
  }

  /** @return The organizationsUrl */
  @JsonProperty("organizations_url")
  public String getOrganizationsUrl() {
    return organizationsUrl;
  }

  /** @param organizationsUrl The organizations_url */
  @JsonProperty("organizations_url")
  public void setOrganizationsUrl(String organizationsUrl) {
    this.organizationsUrl = organizationsUrl;
  }

  /** @return The reposUrl */
  @JsonProperty("repos_url")
  public String getReposUrl() {
    return reposUrl;
  }

  /** @param reposUrl The repos_url */
  @JsonProperty("repos_url")
  public void setReposUrl(String reposUrl) {
    this.reposUrl = reposUrl;
  }

  /** @return The eventsUrl */
  @JsonProperty("events_url")
  public String getEventsUrl() {
    return eventsUrl;
  }

  /** @param eventsUrl The events_url */
  @JsonProperty("events_url")
  public void setEventsUrl(String eventsUrl) {
    this.eventsUrl = eventsUrl;
  }

  /** @return The receivedEventsUrl */
  @JsonProperty("received_events_url")
  public String getReceivedEventsUrl() {
    return receivedEventsUrl;
  }

  /** @param receivedEventsUrl The received_events_url */
  @JsonProperty("received_events_url")
  public void setReceivedEventsUrl(String receivedEventsUrl) {
    this.receivedEventsUrl = receivedEventsUrl;
  }

  /** @return The type */
  @JsonProperty("type")
  public String getType() {
    return type;
  }

  /** @param type The type */
  @JsonProperty("type")
  public void setType(String type) {
    this.type = type;
  }

  /** @return The siteAdmin */
  @JsonProperty("site_admin")
  public Boolean getSiteAdmin() {
    return siteAdmin;
  }

  /** @param siteAdmin The site_admin */
  @JsonProperty("site_admin")
  public void setSiteAdmin(Boolean siteAdmin) {
    this.siteAdmin = siteAdmin;
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
