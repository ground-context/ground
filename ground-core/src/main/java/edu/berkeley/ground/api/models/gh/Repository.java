
package edu.berkeley.ground.api.models.gh;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Generated;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Generated("org.jsonschema2pojo")
@JsonPropertyOrder({
    "id",
    "name",
    "full_name",
    "owner",
    "private",
    "html_url",
    "description",
    "fork",
    "url",
    "forks_url",
    "keys_url",
    "collaborators_url",
    "teams_url",
    "hooks_url",
    "issue_events_url",
    "events_url",
    "assignees_url",
    "branches_url",
    "tags_url",
    "blobs_url",
    "git_tags_url",
    "git_refs_url",
    "trees_url",
    "statuses_url",
    "languages_url",
    "stargazers_url",
    "contributors_url",
    "subscribers_url",
    "subscription_url",
    "commits_url",
    "git_commits_url",
    "comments_url",
    "issue_comment_url",
    "contents_url",
    "compare_url",
    "merges_url",
    "archive_url",
    "downloads_url",
    "issues_url",
    "pulls_url",
    "milestones_url",
    "notifications_url",
    "labels_url",
    "releases_url",
    "created_at",
    "updated_at",
    "pushed_at",
    "git_url",
    "ssh_url",
    "clone_url",
    "svn_url",
    "homepage",
    "size",
    "stargazers_count",
    "watchers_count",
    "language",
    "has_issues",
    "has_downloads",
    "has_wiki",
    "has_pages",
    "forks_count",
    "mirror_url",
    "open_issues_count",
    "forks",
    "open_issues",
    "watchers",
    "default_branch",
    "stargazers",
    "master_branch"
})
public class Repository {

    @JsonProperty("id")
    private Integer id;
    @JsonProperty("name")
    private String name;
    @JsonProperty("full_name")
    private String fullName;
    @JsonProperty("owner")
    private Owner owner;
    @JsonProperty("private")
    private Boolean _private;
    @JsonProperty("html_url")
    private String htmlUrl;
    @JsonProperty("description")
    private String description;
    @JsonProperty("fork")
    private Boolean fork;
    @JsonProperty("url")
    private String url;
    @JsonProperty("forks_url")
    private String forksUrl;
    @JsonProperty("keys_url")
    private String keysUrl;
    @JsonProperty("collaborators_url")
    private String collaboratorsUrl;
    @JsonProperty("teams_url")
    private String teamsUrl;
    @JsonProperty("hooks_url")
    private String hooksUrl;
    @JsonProperty("issue_events_url")
    private String issueEventsUrl;
    @JsonProperty("events_url")
    private String eventsUrl;
    @JsonProperty("assignees_url")
    private String assigneesUrl;
    @JsonProperty("branches_url")
    private String branchesUrl;
    @JsonProperty("tags_url")
    private String tagsUrl;
    @JsonProperty("blobs_url")
    private String blobsUrl;
    @JsonProperty("git_tags_url")
    private String gitTagsUrl;
    @JsonProperty("git_refs_url")
    private String gitRefsUrl;
    @JsonProperty("trees_url")
    private String treesUrl;
    @JsonProperty("statuses_url")
    private String statusesUrl;
    @JsonProperty("languages_url")
    private String languagesUrl;
    @JsonProperty("stargazers_url")
    private String stargazersUrl;
    @JsonProperty("contributors_url")
    private String contributorsUrl;
    @JsonProperty("subscribers_url")
    private String subscribersUrl;
    @JsonProperty("subscription_url")
    private String subscriptionUrl;
    @JsonProperty("commits_url")
    private String commitsUrl;
    @JsonProperty("git_commits_url")
    private String gitCommitsUrl;
    @JsonProperty("comments_url")
    private String commentsUrl;
    @JsonProperty("issue_comment_url")
    private String issueCommentUrl;
    @JsonProperty("contents_url")
    private String contentsUrl;
    @JsonProperty("compare_url")
    private String compareUrl;
    @JsonProperty("merges_url")
    private String mergesUrl;
    @JsonProperty("archive_url")
    private String archiveUrl;
    @JsonProperty("downloads_url")
    private String downloadsUrl;
    @JsonProperty("issues_url")
    private String issuesUrl;
    @JsonProperty("pulls_url")
    private String pullsUrl;
    @JsonProperty("milestones_url")
    private String milestonesUrl;
    @JsonProperty("notifications_url")
    private String notificationsUrl;
    @JsonProperty("labels_url")
    private String labelsUrl;
    @JsonProperty("releases_url")
    private String releasesUrl;
    @JsonProperty("created_at")
    private Integer createdAt;
    @JsonProperty("updated_at")
    private String updatedAt;
    @JsonProperty("pushed_at")
    private Integer pushedAt;
    @JsonProperty("git_url")
    private String gitUrl;
    @JsonProperty("ssh_url")
    private String sshUrl;
    @JsonProperty("clone_url")
    private String cloneUrl;
    @JsonProperty("svn_url")
    private String svnUrl;
    @JsonProperty("homepage")
    private Object homepage;
    @JsonProperty("size")
    private Integer size;
    @JsonProperty("stargazers_count")
    private Integer stargazersCount;
    @JsonProperty("watchers_count")
    private Integer watchersCount;
    @JsonProperty("language")
    private Object language;
    @JsonProperty("has_issues")
    private Boolean hasIssues;
    @JsonProperty("has_downloads")
    private Boolean hasDownloads;
    @JsonProperty("has_wiki")
    private Boolean hasWiki;
    @JsonProperty("has_pages")
    private Boolean hasPages;
    @JsonProperty("forks_count")
    private Integer forksCount;
    @JsonProperty("mirror_url")
    private Object mirrorUrl;
    @JsonProperty("open_issues_count")
    private Integer openIssuesCount;
    @JsonProperty("forks")
    private Integer forks;
    @JsonProperty("open_issues")
    private Integer openIssues;
    @JsonProperty("watchers")
    private Integer watchers;
    @JsonProperty("default_branch")
    private String defaultBranch;
    @JsonProperty("stargazers")
    private Integer stargazers;
    @JsonProperty("master_branch")
    private String masterBranch;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    /**
     * No args constructor for use in serialization
     * 
     */
    public Repository() {
    }

    /**
     * 
     * @param notificationsUrl
     * @param releasesUrl
     * @param pushedAt
     * @param gitTagsUrl
     * @param contentsUrl
     * @param blobsUrl
     * @param masterBranch
     * @param issueEventsUrl
     * @param htmlUrl
     * @param _private
     * @param hooksUrl
     * @param description
     * @param commentsUrl
     * @param commitsUrl
     * @param labelsUrl
     * @param assigneesUrl
     * @param mergesUrl
     * @param fork
     * @param compareUrl
     * @param stargazersUrl
     * @param gitRefsUrl
     * @param watchersCount
     * @param openIssuesCount
     * @param mirrorUrl
     * @param homepage
     * @param url
     * @param size
     * @param keysUrl
     * @param gitCommitsUrl
     * @param milestonesUrl
     * @param downloadsUrl
     * @param issueCommentUrl
     * @param pullsUrl
     * @param owner
     * @param forksUrl
     * @param language
     * @param statusesUrl
     * @param eventsUrl
     * @param openIssues
     * @param teamsUrl
     * @param sshUrl
     * @param contributorsUrl
     * @param stargazersCount
     * @param tagsUrl
     * @param id
     * @param hasIssues
     * @param createdAt
     * @param name
     * @param treesUrl
     * @param cloneUrl
     * @param issuesUrl
     * @param gitUrl
     * @param forksCount
     * @param watchers
     * @param subscriptionUrl
     * @param svnUrl
     * @param archiveUrl
     * @param hasPages
     * @param languagesUrl
     * @param updatedAt
     * @param collaboratorsUrl
     * @param forks
     * @param hasDownloads
     * @param subscribersUrl
     * @param branchesUrl
     * @param stargazers
     * @param fullName
     * @param hasWiki
     * @param defaultBranch
     */
    public Repository(Integer id, String name, String fullName, Owner owner, Boolean _private, String htmlUrl, String description, Boolean fork, String url, String forksUrl, String keysUrl, String collaboratorsUrl, String teamsUrl, String hooksUrl, String issueEventsUrl, String eventsUrl, String assigneesUrl, String branchesUrl, String tagsUrl, String blobsUrl, String gitTagsUrl, String gitRefsUrl, String treesUrl, String statusesUrl, String languagesUrl, String stargazersUrl, String contributorsUrl, String subscribersUrl, String subscriptionUrl, String commitsUrl, String gitCommitsUrl, String commentsUrl, String issueCommentUrl, String contentsUrl, String compareUrl, String mergesUrl, String archiveUrl, String downloadsUrl, String issuesUrl, String pullsUrl, String milestonesUrl, String notificationsUrl, String labelsUrl, String releasesUrl, Integer createdAt, String updatedAt, Integer pushedAt, String gitUrl, String sshUrl, String cloneUrl, String svnUrl, Object homepage, Integer size, Integer stargazersCount, Integer watchersCount, Object language, Boolean hasIssues, Boolean hasDownloads, Boolean hasWiki, Boolean hasPages, Integer forksCount, Object mirrorUrl, Integer openIssuesCount, Integer forks, Integer openIssues, Integer watchers, String defaultBranch, Integer stargazers, String masterBranch) {
        this.id = id;
        this.name = name;
        this.fullName = fullName;
        this.owner = owner;
        this._private = _private;
        this.htmlUrl = htmlUrl;
        this.description = description;
        this.fork = fork;
        this.url = url;
        this.forksUrl = forksUrl;
        this.keysUrl = keysUrl;
        this.collaboratorsUrl = collaboratorsUrl;
        this.teamsUrl = teamsUrl;
        this.hooksUrl = hooksUrl;
        this.issueEventsUrl = issueEventsUrl;
        this.eventsUrl = eventsUrl;
        this.assigneesUrl = assigneesUrl;
        this.branchesUrl = branchesUrl;
        this.tagsUrl = tagsUrl;
        this.blobsUrl = blobsUrl;
        this.gitTagsUrl = gitTagsUrl;
        this.gitRefsUrl = gitRefsUrl;
        this.treesUrl = treesUrl;
        this.statusesUrl = statusesUrl;
        this.languagesUrl = languagesUrl;
        this.stargazersUrl = stargazersUrl;
        this.contributorsUrl = contributorsUrl;
        this.subscribersUrl = subscribersUrl;
        this.subscriptionUrl = subscriptionUrl;
        this.commitsUrl = commitsUrl;
        this.gitCommitsUrl = gitCommitsUrl;
        this.commentsUrl = commentsUrl;
        this.issueCommentUrl = issueCommentUrl;
        this.contentsUrl = contentsUrl;
        this.compareUrl = compareUrl;
        this.mergesUrl = mergesUrl;
        this.archiveUrl = archiveUrl;
        this.downloadsUrl = downloadsUrl;
        this.issuesUrl = issuesUrl;
        this.pullsUrl = pullsUrl;
        this.milestonesUrl = milestonesUrl;
        this.notificationsUrl = notificationsUrl;
        this.labelsUrl = labelsUrl;
        this.releasesUrl = releasesUrl;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.pushedAt = pushedAt;
        this.gitUrl = gitUrl;
        this.sshUrl = sshUrl;
        this.cloneUrl = cloneUrl;
        this.svnUrl = svnUrl;
        this.homepage = homepage;
        this.size = size;
        this.stargazersCount = stargazersCount;
        this.watchersCount = watchersCount;
        this.language = language;
        this.hasIssues = hasIssues;
        this.hasDownloads = hasDownloads;
        this.hasWiki = hasWiki;
        this.hasPages = hasPages;
        this.forksCount = forksCount;
        this.mirrorUrl = mirrorUrl;
        this.openIssuesCount = openIssuesCount;
        this.forks = forks;
        this.openIssues = openIssues;
        this.watchers = watchers;
        this.defaultBranch = defaultBranch;
        this.stargazers = stargazers;
        this.masterBranch = masterBranch;
    }

    /**
     * 
     * @return
     *     The id
     */
    @JsonProperty("id")
    public Integer getId() {
        return id;
    }

    /**
     * 
     * @param id
     *     The id
     */
    @JsonProperty("id")
    public void setId(Integer id) {
        this.id = id;
    }

    /**
     * 
     * @return
     *     The name
     */
    @JsonProperty("name")
    public String getName() {
        return name;
    }

    /**
     * 
     * @param name
     *     The name
     */
    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    /**
     * 
     * @return
     *     The fullName
     */
    @JsonProperty("full_name")
    public String getFullName() {
        return fullName;
    }

    /**
     * 
     * @param fullName
     *     The full_name
     */
    @JsonProperty("full_name")
    public void setFullName(String fullName) {
        this.fullName = fullName;
    }

    /**
     * 
     * @return
     *     The owner
     */
    @JsonProperty("owner")
    public Owner getOwner() {
        return owner;
    }

    /**
     * 
     * @param owner
     *     The owner
     */
    @JsonProperty("owner")
    public void setOwner(Owner owner) {
        this.owner = owner;
    }

    /**
     * 
     * @return
     *     The _private
     */
    @JsonProperty("private")
    public Boolean getPrivate() {
        return _private;
    }

    /**
     * 
     * @param _private
     *     The private
     */
    @JsonProperty("private")
    public void setPrivate(Boolean _private) {
        this._private = _private;
    }

    /**
     * 
     * @return
     *     The htmlUrl
     */
    @JsonProperty("html_url")
    public String getHtmlUrl() {
        return htmlUrl;
    }

    /**
     * 
     * @param htmlUrl
     *     The html_url
     */
    @JsonProperty("html_url")
    public void setHtmlUrl(String htmlUrl) {
        this.htmlUrl = htmlUrl;
    }

    /**
     * 
     * @return
     *     The description
     */
    @JsonProperty("description")
    public String getDescription() {
        return description;
    }

    /**
     * 
     * @param description
     *     The description
     */
    @JsonProperty("description")
    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * 
     * @return
     *     The fork
     */
    @JsonProperty("fork")
    public Boolean getFork() {
        return fork;
    }

    /**
     * 
     * @param fork
     *     The fork
     */
    @JsonProperty("fork")
    public void setFork(Boolean fork) {
        this.fork = fork;
    }

    /**
     * 
     * @return
     *     The url
     */
    @JsonProperty("url")
    public String getUrl() {
        return url;
    }

    /**
     * 
     * @param url
     *     The url
     */
    @JsonProperty("url")
    public void setUrl(String url) {
        this.url = url;
    }

    /**
     * 
     * @return
     *     The forksUrl
     */
    @JsonProperty("forks_url")
    public String getForksUrl() {
        return forksUrl;
    }

    /**
     * 
     * @param forksUrl
     *     The forks_url
     */
    @JsonProperty("forks_url")
    public void setForksUrl(String forksUrl) {
        this.forksUrl = forksUrl;
    }

    /**
     * 
     * @return
     *     The keysUrl
     */
    @JsonProperty("keys_url")
    public String getKeysUrl() {
        return keysUrl;
    }

    /**
     * 
     * @param keysUrl
     *     The keys_url
     */
    @JsonProperty("keys_url")
    public void setKeysUrl(String keysUrl) {
        this.keysUrl = keysUrl;
    }

    /**
     * 
     * @return
     *     The collaboratorsUrl
     */
    @JsonProperty("collaborators_url")
    public String getCollaboratorsUrl() {
        return collaboratorsUrl;
    }

    /**
     * 
     * @param collaboratorsUrl
     *     The collaborators_url
     */
    @JsonProperty("collaborators_url")
    public void setCollaboratorsUrl(String collaboratorsUrl) {
        this.collaboratorsUrl = collaboratorsUrl;
    }

    /**
     * 
     * @return
     *     The teamsUrl
     */
    @JsonProperty("teams_url")
    public String getTeamsUrl() {
        return teamsUrl;
    }

    /**
     * 
     * @param teamsUrl
     *     The teams_url
     */
    @JsonProperty("teams_url")
    public void setTeamsUrl(String teamsUrl) {
        this.teamsUrl = teamsUrl;
    }

    /**
     * 
     * @return
     *     The hooksUrl
     */
    @JsonProperty("hooks_url")
    public String getHooksUrl() {
        return hooksUrl;
    }

    /**
     * 
     * @param hooksUrl
     *     The hooks_url
     */
    @JsonProperty("hooks_url")
    public void setHooksUrl(String hooksUrl) {
        this.hooksUrl = hooksUrl;
    }

    /**
     * 
     * @return
     *     The issueEventsUrl
     */
    @JsonProperty("issue_events_url")
    public String getIssueEventsUrl() {
        return issueEventsUrl;
    }

    /**
     * 
     * @param issueEventsUrl
     *     The issue_events_url
     */
    @JsonProperty("issue_events_url")
    public void setIssueEventsUrl(String issueEventsUrl) {
        this.issueEventsUrl = issueEventsUrl;
    }

    /**
     * 
     * @return
     *     The eventsUrl
     */
    @JsonProperty("events_url")
    public String getEventsUrl() {
        return eventsUrl;
    }

    /**
     * 
     * @param eventsUrl
     *     The events_url
     */
    @JsonProperty("events_url")
    public void setEventsUrl(String eventsUrl) {
        this.eventsUrl = eventsUrl;
    }

    /**
     * 
     * @return
     *     The assigneesUrl
     */
    @JsonProperty("assignees_url")
    public String getAssigneesUrl() {
        return assigneesUrl;
    }

    /**
     * 
     * @param assigneesUrl
     *     The assignees_url
     */
    @JsonProperty("assignees_url")
    public void setAssigneesUrl(String assigneesUrl) {
        this.assigneesUrl = assigneesUrl;
    }

    /**
     * 
     * @return
     *     The branchesUrl
     */
    @JsonProperty("branches_url")
    public String getBranchesUrl() {
        return branchesUrl;
    }

    /**
     * 
     * @param branchesUrl
     *     The branches_url
     */
    @JsonProperty("branches_url")
    public void setBranchesUrl(String branchesUrl) {
        this.branchesUrl = branchesUrl;
    }

    /**
     * 
     * @return
     *     The tagsUrl
     */
    @JsonProperty("tags_url")
    public String getTagsUrl() {
        return tagsUrl;
    }

    /**
     * 
     * @param tagsUrl
     *     The tags_url
     */
    @JsonProperty("tags_url")
    public void setTagsUrl(String tagsUrl) {
        this.tagsUrl = tagsUrl;
    }

    /**
     * 
     * @return
     *     The blobsUrl
     */
    @JsonProperty("blobs_url")
    public String getBlobsUrl() {
        return blobsUrl;
    }

    /**
     * 
     * @param blobsUrl
     *     The blobs_url
     */
    @JsonProperty("blobs_url")
    public void setBlobsUrl(String blobsUrl) {
        this.blobsUrl = blobsUrl;
    }

    /**
     * 
     * @return
     *     The gitTagsUrl
     */
    @JsonProperty("git_tags_url")
    public String getGitTagsUrl() {
        return gitTagsUrl;
    }

    /**
     * 
     * @param gitTagsUrl
     *     The git_tags_url
     */
    @JsonProperty("git_tags_url")
    public void setGitTagsUrl(String gitTagsUrl) {
        this.gitTagsUrl = gitTagsUrl;
    }

    /**
     * 
     * @return
     *     The gitRefsUrl
     */
    @JsonProperty("git_refs_url")
    public String getGitRefsUrl() {
        return gitRefsUrl;
    }

    /**
     * 
     * @param gitRefsUrl
     *     The git_refs_url
     */
    @JsonProperty("git_refs_url")
    public void setGitRefsUrl(String gitRefsUrl) {
        this.gitRefsUrl = gitRefsUrl;
    }

    /**
     * 
     * @return
     *     The treesUrl
     */
    @JsonProperty("trees_url")
    public String getTreesUrl() {
        return treesUrl;
    }

    /**
     * 
     * @param treesUrl
     *     The trees_url
     */
    @JsonProperty("trees_url")
    public void setTreesUrl(String treesUrl) {
        this.treesUrl = treesUrl;
    }

    /**
     * 
     * @return
     *     The statusesUrl
     */
    @JsonProperty("statuses_url")
    public String getStatusesUrl() {
        return statusesUrl;
    }

    /**
     * 
     * @param statusesUrl
     *     The statuses_url
     */
    @JsonProperty("statuses_url")
    public void setStatusesUrl(String statusesUrl) {
        this.statusesUrl = statusesUrl;
    }

    /**
     * 
     * @return
     *     The languagesUrl
     */
    @JsonProperty("languages_url")
    public String getLanguagesUrl() {
        return languagesUrl;
    }

    /**
     * 
     * @param languagesUrl
     *     The languages_url
     */
    @JsonProperty("languages_url")
    public void setLanguagesUrl(String languagesUrl) {
        this.languagesUrl = languagesUrl;
    }

    /**
     * 
     * @return
     *     The stargazersUrl
     */
    @JsonProperty("stargazers_url")
    public String getStargazersUrl() {
        return stargazersUrl;
    }

    /**
     * 
     * @param stargazersUrl
     *     The stargazers_url
     */
    @JsonProperty("stargazers_url")
    public void setStargazersUrl(String stargazersUrl) {
        this.stargazersUrl = stargazersUrl;
    }

    /**
     * 
     * @return
     *     The contributorsUrl
     */
    @JsonProperty("contributors_url")
    public String getContributorsUrl() {
        return contributorsUrl;
    }

    /**
     * 
     * @param contributorsUrl
     *     The contributors_url
     */
    @JsonProperty("contributors_url")
    public void setContributorsUrl(String contributorsUrl) {
        this.contributorsUrl = contributorsUrl;
    }

    /**
     * 
     * @return
     *     The subscribersUrl
     */
    @JsonProperty("subscribers_url")
    public String getSubscribersUrl() {
        return subscribersUrl;
    }

    /**
     * 
     * @param subscribersUrl
     *     The subscribers_url
     */
    @JsonProperty("subscribers_url")
    public void setSubscribersUrl(String subscribersUrl) {
        this.subscribersUrl = subscribersUrl;
    }

    /**
     * 
     * @return
     *     The subscriptionUrl
     */
    @JsonProperty("subscription_url")
    public String getSubscriptionUrl() {
        return subscriptionUrl;
    }

    /**
     * 
     * @param subscriptionUrl
     *     The subscription_url
     */
    @JsonProperty("subscription_url")
    public void setSubscriptionUrl(String subscriptionUrl) {
        this.subscriptionUrl = subscriptionUrl;
    }

    /**
     * 
     * @return
     *     The commitsUrl
     */
    @JsonProperty("commits_url")
    public String getCommitsUrl() {
        return commitsUrl;
    }

    /**
     * 
     * @param commitsUrl
     *     The commits_url
     */
    @JsonProperty("commits_url")
    public void setCommitsUrl(String commitsUrl) {
        this.commitsUrl = commitsUrl;
    }

    /**
     * 
     * @return
     *     The gitCommitsUrl
     */
    @JsonProperty("git_commits_url")
    public String getGitCommitsUrl() {
        return gitCommitsUrl;
    }

    /**
     * 
     * @param gitCommitsUrl
     *     The git_commits_url
     */
    @JsonProperty("git_commits_url")
    public void setGitCommitsUrl(String gitCommitsUrl) {
        this.gitCommitsUrl = gitCommitsUrl;
    }

    /**
     * 
     * @return
     *     The commentsUrl
     */
    @JsonProperty("comments_url")
    public String getCommentsUrl() {
        return commentsUrl;
    }

    /**
     * 
     * @param commentsUrl
     *     The comments_url
     */
    @JsonProperty("comments_url")
    public void setCommentsUrl(String commentsUrl) {
        this.commentsUrl = commentsUrl;
    }

    /**
     * 
     * @return
     *     The issueCommentUrl
     */
    @JsonProperty("issue_comment_url")
    public String getIssueCommentUrl() {
        return issueCommentUrl;
    }

    /**
     * 
     * @param issueCommentUrl
     *     The issue_comment_url
     */
    @JsonProperty("issue_comment_url")
    public void setIssueCommentUrl(String issueCommentUrl) {
        this.issueCommentUrl = issueCommentUrl;
    }

    /**
     * 
     * @return
     *     The contentsUrl
     */
    @JsonProperty("contents_url")
    public String getContentsUrl() {
        return contentsUrl;
    }

    /**
     * 
     * @param contentsUrl
     *     The contents_url
     */
    @JsonProperty("contents_url")
    public void setContentsUrl(String contentsUrl) {
        this.contentsUrl = contentsUrl;
    }

    /**
     * 
     * @return
     *     The compareUrl
     */
    @JsonProperty("compare_url")
    public String getCompareUrl() {
        return compareUrl;
    }

    /**
     * 
     * @param compareUrl
     *     The compare_url
     */
    @JsonProperty("compare_url")
    public void setCompareUrl(String compareUrl) {
        this.compareUrl = compareUrl;
    }

    /**
     * 
     * @return
     *     The mergesUrl
     */
    @JsonProperty("merges_url")
    public String getMergesUrl() {
        return mergesUrl;
    }

    /**
     * 
     * @param mergesUrl
     *     The merges_url
     */
    @JsonProperty("merges_url")
    public void setMergesUrl(String mergesUrl) {
        this.mergesUrl = mergesUrl;
    }

    /**
     * 
     * @return
     *     The archiveUrl
     */
    @JsonProperty("archive_url")
    public String getArchiveUrl() {
        return archiveUrl;
    }

    /**
     * 
     * @param archiveUrl
     *     The archive_url
     */
    @JsonProperty("archive_url")
    public void setArchiveUrl(String archiveUrl) {
        this.archiveUrl = archiveUrl;
    }

    /**
     * 
     * @return
     *     The downloadsUrl
     */
    @JsonProperty("downloads_url")
    public String getDownloadsUrl() {
        return downloadsUrl;
    }

    /**
     * 
     * @param downloadsUrl
     *     The downloads_url
     */
    @JsonProperty("downloads_url")
    public void setDownloadsUrl(String downloadsUrl) {
        this.downloadsUrl = downloadsUrl;
    }

    /**
     * 
     * @return
     *     The issuesUrl
     */
    @JsonProperty("issues_url")
    public String getIssuesUrl() {
        return issuesUrl;
    }

    /**
     * 
     * @param issuesUrl
     *     The issues_url
     */
    @JsonProperty("issues_url")
    public void setIssuesUrl(String issuesUrl) {
        this.issuesUrl = issuesUrl;
    }

    /**
     * 
     * @return
     *     The pullsUrl
     */
    @JsonProperty("pulls_url")
    public String getPullsUrl() {
        return pullsUrl;
    }

    /**
     * 
     * @param pullsUrl
     *     The pulls_url
     */
    @JsonProperty("pulls_url")
    public void setPullsUrl(String pullsUrl) {
        this.pullsUrl = pullsUrl;
    }

    /**
     * 
     * @return
     *     The milestonesUrl
     */
    @JsonProperty("milestones_url")
    public String getMilestonesUrl() {
        return milestonesUrl;
    }

    /**
     * 
     * @param milestonesUrl
     *     The milestones_url
     */
    @JsonProperty("milestones_url")
    public void setMilestonesUrl(String milestonesUrl) {
        this.milestonesUrl = milestonesUrl;
    }

    /**
     * 
     * @return
     *     The notificationsUrl
     */
    @JsonProperty("notifications_url")
    public String getNotificationsUrl() {
        return notificationsUrl;
    }

    /**
     * 
     * @param notificationsUrl
     *     The notifications_url
     */
    @JsonProperty("notifications_url")
    public void setNotificationsUrl(String notificationsUrl) {
        this.notificationsUrl = notificationsUrl;
    }

    /**
     * 
     * @return
     *     The labelsUrl
     */
    @JsonProperty("labels_url")
    public String getLabelsUrl() {
        return labelsUrl;
    }

    /**
     * 
     * @param labelsUrl
     *     The labels_url
     */
    @JsonProperty("labels_url")
    public void setLabelsUrl(String labelsUrl) {
        this.labelsUrl = labelsUrl;
    }

    /**
     * 
     * @return
     *     The releasesUrl
     */
    @JsonProperty("releases_url")
    public String getReleasesUrl() {
        return releasesUrl;
    }

    /**
     * 
     * @param releasesUrl
     *     The releases_url
     */
    @JsonProperty("releases_url")
    public void setReleasesUrl(String releasesUrl) {
        this.releasesUrl = releasesUrl;
    }

    /**
     * 
     * @return
     *     The createdAt
     */
    @JsonProperty("created_at")
    public Integer getCreatedAt() {
        return createdAt;
    }

    /**
     * 
     * @param createdAt
     *     The created_at
     */
    @JsonProperty("created_at")
    public void setCreatedAt(Integer createdAt) {
        this.createdAt = createdAt;
    }

    /**
     * 
     * @return
     *     The updatedAt
     */
    @JsonProperty("updated_at")
    public String getUpdatedAt() {
        return updatedAt;
    }

    /**
     * 
     * @param updatedAt
     *     The updated_at
     */
    @JsonProperty("updated_at")
    public void setUpdatedAt(String updatedAt) {
        this.updatedAt = updatedAt;
    }

    /**
     * 
     * @return
     *     The pushedAt
     */
    @JsonProperty("pushed_at")
    public Integer getPushedAt() {
        return pushedAt;
    }

    /**
     * 
     * @param pushedAt
     *     The pushed_at
     */
    @JsonProperty("pushed_at")
    public void setPushedAt(Integer pushedAt) {
        this.pushedAt = pushedAt;
    }

    /**
     * 
     * @return
     *     The gitUrl
     */
    @JsonProperty("git_url")
    public String getGitUrl() {
        return gitUrl;
    }

    /**
     * 
     * @param gitUrl
     *     The git_url
     */
    @JsonProperty("git_url")
    public void setGitUrl(String gitUrl) {
        this.gitUrl = gitUrl;
    }

    /**
     * 
     * @return
     *     The sshUrl
     */
    @JsonProperty("ssh_url")
    public String getSshUrl() {
        return sshUrl;
    }

    /**
     * 
     * @param sshUrl
     *     The ssh_url
     */
    @JsonProperty("ssh_url")
    public void setSshUrl(String sshUrl) {
        this.sshUrl = sshUrl;
    }

    /**
     * 
     * @return
     *     The cloneUrl
     */
    @JsonProperty("clone_url")
    public String getCloneUrl() {
        return cloneUrl;
    }

    /**
     * 
     * @param cloneUrl
     *     The clone_url
     */
    @JsonProperty("clone_url")
    public void setCloneUrl(String cloneUrl) {
        this.cloneUrl = cloneUrl;
    }

    /**
     * 
     * @return
     *     The svnUrl
     */
    @JsonProperty("svn_url")
    public String getSvnUrl() {
        return svnUrl;
    }

    /**
     * 
     * @param svnUrl
     *     The svn_url
     */
    @JsonProperty("svn_url")
    public void setSvnUrl(String svnUrl) {
        this.svnUrl = svnUrl;
    }

    /**
     * 
     * @return
     *     The homepage
     */
    @JsonProperty("homepage")
    public Object getHomepage() {
        return homepage;
    }

    /**
     * 
     * @param homepage
     *     The homepage
     */
    @JsonProperty("homepage")
    public void setHomepage(Object homepage) {
        this.homepage = homepage;
    }

    /**
     * 
     * @return
     *     The size
     */
    @JsonProperty("size")
    public Integer getSize() {
        return size;
    }

    /**
     * 
     * @param size
     *     The size
     */
    @JsonProperty("size")
    public void setSize(Integer size) {
        this.size = size;
    }

    /**
     * 
     * @return
     *     The stargazersCount
     */
    @JsonProperty("stargazers_count")
    public Integer getStargazersCount() {
        return stargazersCount;
    }

    /**
     * 
     * @param stargazersCount
     *     The stargazers_count
     */
    @JsonProperty("stargazers_count")
    public void setStargazersCount(Integer stargazersCount) {
        this.stargazersCount = stargazersCount;
    }

    /**
     * 
     * @return
     *     The watchersCount
     */
    @JsonProperty("watchers_count")
    public Integer getWatchersCount() {
        return watchersCount;
    }

    /**
     * 
     * @param watchersCount
     *     The watchers_count
     */
    @JsonProperty("watchers_count")
    public void setWatchersCount(Integer watchersCount) {
        this.watchersCount = watchersCount;
    }

    /**
     * 
     * @return
     *     The language
     */
    @JsonProperty("language")
    public Object getLanguage() {
        return language;
    }

    /**
     * 
     * @param language
     *     The language
     */
    @JsonProperty("language")
    public void setLanguage(Object language) {
        this.language = language;
    }

    /**
     * 
     * @return
     *     The hasIssues
     */
    @JsonProperty("has_issues")
    public Boolean getHasIssues() {
        return hasIssues;
    }

    /**
     * 
     * @param hasIssues
     *     The has_issues
     */
    @JsonProperty("has_issues")
    public void setHasIssues(Boolean hasIssues) {
        this.hasIssues = hasIssues;
    }

    /**
     * 
     * @return
     *     The hasDownloads
     */
    @JsonProperty("has_downloads")
    public Boolean getHasDownloads() {
        return hasDownloads;
    }

    /**
     * 
     * @param hasDownloads
     *     The has_downloads
     */
    @JsonProperty("has_downloads")
    public void setHasDownloads(Boolean hasDownloads) {
        this.hasDownloads = hasDownloads;
    }

    /**
     * 
     * @return
     *     The hasWiki
     */
    @JsonProperty("has_wiki")
    public Boolean getHasWiki() {
        return hasWiki;
    }

    /**
     * 
     * @param hasWiki
     *     The has_wiki
     */
    @JsonProperty("has_wiki")
    public void setHasWiki(Boolean hasWiki) {
        this.hasWiki = hasWiki;
    }

    /**
     * 
     * @return
     *     The hasPages
     */
    @JsonProperty("has_pages")
    public Boolean getHasPages() {
        return hasPages;
    }

    /**
     * 
     * @param hasPages
     *     The has_pages
     */
    @JsonProperty("has_pages")
    public void setHasPages(Boolean hasPages) {
        this.hasPages = hasPages;
    }

    /**
     * 
     * @return
     *     The forksCount
     */
    @JsonProperty("forks_count")
    public Integer getForksCount() {
        return forksCount;
    }

    /**
     * 
     * @param forksCount
     *     The forks_count
     */
    @JsonProperty("forks_count")
    public void setForksCount(Integer forksCount) {
        this.forksCount = forksCount;
    }

    /**
     * 
     * @return
     *     The mirrorUrl
     */
    @JsonProperty("mirror_url")
    public Object getMirrorUrl() {
        return mirrorUrl;
    }

    /**
     * 
     * @param mirrorUrl
     *     The mirror_url
     */
    @JsonProperty("mirror_url")
    public void setMirrorUrl(Object mirrorUrl) {
        this.mirrorUrl = mirrorUrl;
    }

    /**
     * 
     * @return
     *     The openIssuesCount
     */
    @JsonProperty("open_issues_count")
    public Integer getOpenIssuesCount() {
        return openIssuesCount;
    }

    /**
     * 
     * @param openIssuesCount
     *     The open_issues_count
     */
    @JsonProperty("open_issues_count")
    public void setOpenIssuesCount(Integer openIssuesCount) {
        this.openIssuesCount = openIssuesCount;
    }

    /**
     * 
     * @return
     *     The forks
     */
    @JsonProperty("forks")
    public Integer getForks() {
        return forks;
    }

    /**
     * 
     * @param forks
     *     The forks
     */
    @JsonProperty("forks")
    public void setForks(Integer forks) {
        this.forks = forks;
    }

    /**
     * 
     * @return
     *     The openIssues
     */
    @JsonProperty("open_issues")
    public Integer getOpenIssues() {
        return openIssues;
    }

    /**
     * 
     * @param openIssues
     *     The open_issues
     */
    @JsonProperty("open_issues")
    public void setOpenIssues(Integer openIssues) {
        this.openIssues = openIssues;
    }

    /**
     * 
     * @return
     *     The watchers
     */
    @JsonProperty("watchers")
    public Integer getWatchers() {
        return watchers;
    }

    /**
     * 
     * @param watchers
     *     The watchers
     */
    @JsonProperty("watchers")
    public void setWatchers(Integer watchers) {
        this.watchers = watchers;
    }

    /**
     * 
     * @return
     *     The defaultBranch
     */
    @JsonProperty("default_branch")
    public String getDefaultBranch() {
        return defaultBranch;
    }

    /**
     * 
     * @param defaultBranch
     *     The default_branch
     */
    @JsonProperty("default_branch")
    public void setDefaultBranch(String defaultBranch) {
        this.defaultBranch = defaultBranch;
    }

    /**
     * 
     * @return
     *     The stargazers
     */
    @JsonProperty("stargazers")
    public Integer getStargazers() {
        return stargazers;
    }

    /**
     * 
     * @param stargazers
     *     The stargazers
     */
    @JsonProperty("stargazers")
    public void setStargazers(Integer stargazers) {
        this.stargazers = stargazers;
    }

    /**
     * 
     * @return
     *     The masterBranch
     */
    @JsonProperty("master_branch")
    public String getMasterBranch() {
        return masterBranch;
    }

    /**
     * 
     * @param masterBranch
     *     The master_branch
     */
    @JsonProperty("master_branch")
    public void setMasterBranch(String masterBranch) {
        this.masterBranch = masterBranch;
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
