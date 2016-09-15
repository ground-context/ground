/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.berkeley.ground.api.models;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import java.util.ArrayList;

import edu.berkeley.ground.api.models.github.Author;
import edu.berkeley.ground.api.models.github.Commit;
import edu.berkeley.ground.api.models.github.Committer;
import edu.berkeley.ground.api.models.github.GithubWebhook;
import edu.berkeley.ground.api.models.github.HeadCommit;
import edu.berkeley.ground.api.models.github.Owner;
import edu.berkeley.ground.api.models.github.Pusher;
import edu.berkeley.ground.api.models.github.Repository;
import edu.berkeley.ground.api.models.github.Sender;
import io.dropwizard.jackson.Jackson;

import static io.dropwizard.testing.FixtureHelpers.fixture;
import static org.assertj.core.api.Assertions.assertThat;

public class GithubWebhookTest {
    private static final ObjectMapper MAPPER = Jackson.newObjectMapper();

    @Test
    public void serializesToJSON() throws Exception {

        final GithubWebhook githubWebhook = new GithubWebhook(
                "refs/heads/changes",
                "9049f1265b7d61be4a8904a9a27120d2064dab3b",
                "0d1a26e67d8f5eaf1f6ba5c57fc3c7d91ac0fd1c",
                false,
                false,
                false,
                null,
                "https://github.com/baxterthehacker/public-repo/compare/9049f1265b7d...0d1a26e67d8f",
                new ArrayList<Commit>() {{
                    add(new Commit(
                                    "0d1a26e67d8f5eaf1f6ba5c57fc3c7d91ac0fd1c",
                                    "f9d2a07e9488b91af2641b26b9407fe22a451433",
                                    true,
                                    "Update README.md",
                                    "2015-05-05T19:40:15-04:00",
                                    "https://github.com/baxterthehacker/public-repo/commit/0d1a26e67d8f5eaf1f6ba5c57fc3c7d91ac0fd1c",
                                    new Author(
                                            "baxterthehacker",
                                            "baxterthehacker@users.noreply.github.com",
                                            "baxterthehacker"
                                    ),
                                    new Committer(
                                            "baxterthehacker",
                                            "baxterthehacker@users.noreply.github.com",
                                            "baxterthehacker"
                                    ),
                                    new ArrayList<>(),
                                    new ArrayList<>(),
                                    new ArrayList<String>() {{
                                        add("README.md");
                                    }}
                            )
                    );
                }},
                new HeadCommit(
                        "0d1a26e67d8f5eaf1f6ba5c57fc3c7d91ac0fd1c",
                        "f9d2a07e9488b91af2641b26b9407fe22a451433",
                        true,
                        "Update README.md",
                        "2015-05-05T19:40:15-04:00",
                        "https://github.com/baxterthehacker/public-repo/commit/0d1a26e67d8f5eaf1f6ba5c57fc3c7d91ac0fd1c",
                        new Author(
                                "baxterthehacker",
                                "baxterthehacker@users.noreply.github.com",
                                "baxterthehacker"
                        ),
                        new Committer(
                                "baxterthehacker",
                                "baxterthehacker@users.noreply.github.com",
                                "baxterthehacker"
                        ),
                        new ArrayList<>(),
                        new ArrayList<>(),
                        new ArrayList<String>() {{
                            add("README.md");
                        }}),
                new Repository(
                        35129377,
                        "public-repo",
                        "baxterthehacker/public-repo",
                        new Owner(
                                "baxterthehacker",
                                "baxterthehacker@users.noreply.github.com"
                        ),
                        false,
                        "https://github.com/baxterthehacker/public-repo",
                        "",
                        false,
                        "https://github.com/baxterthehacker/public-repo",
                        "https://api.github.com/repos/baxterthehacker/public-repo/forks",
                        "https://api.github.com/repos/baxterthehacker/public-repo/keys{/key_id}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/collaborators{/collaborator}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/teams",
                        "https://api.github.com/repos/baxterthehacker/public-repo/hooks",
                        "https://api.github.com/repos/baxterthehacker/public-repo/issues/events{/number}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/events",
                        "https://api.github.com/repos/baxterthehacker/public-repo/assignees{/user}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/branches{/branch}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/tags",
                        "https://api.github.com/repos/baxterthehacker/public-repo/git/blobs{/sha}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/git/tags{/sha}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/git/refs{/sha}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/git/trees{/sha}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/statuses/{sha}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/languages",
                        "https://api.github.com/repos/baxterthehacker/public-repo/stargazers",
                        "https://api.github.com/repos/baxterthehacker/public-repo/contributors",
                        "https://api.github.com/repos/baxterthehacker/public-repo/subscribers",
                        "https://api.github.com/repos/baxterthehacker/public-repo/subscription",
                        "https://api.github.com/repos/baxterthehacker/public-repo/commits{/sha}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/git/commits{/sha}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/comments{/number}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/issues/comments{/number}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/contents/{+path}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/compare/{base}...{head}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/merges",
                        "https://api.github.com/repos/baxterthehacker/public-repo/{archive_format}{/ref}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/downloads",
                        "https://api.github.com/repos/baxterthehacker/public-repo/issues{/number}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/pulls{/number}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/milestones{/number}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/notifications{?since,all,participating}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/labels{/name}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/releases{/id}",
                        1430869212,
                        "2015-05-05T23:40:12Z",
                        1430869217,
                        "git://github.com/baxterthehacker/public-repo.git",
                        "git@github.com:baxterthehacker/public-repo.git",
                        "https://github.com/baxterthehacker/public-repo.git",
                        "https://github.com/baxterthehacker/public-repo",
                        null,
                        0,
                        0,
                        0,
                        null,
                        true,
                        true,
                        true,
                        true,
                        0,
                        null,
                        0,
                        0,
                        0,
                        0,
                        "master",
                        0,
                        "master"
                ),
                new Pusher(
                        "baxterthehacker",
                        "baxterthehacker@users.noreply.github.com"
                ),
                new Sender(
                        "baxterthehacker",
                        6752317,
                        "https://avatars.githubusercontent.com/u/6752317?v=3",
                        "",
                        "https://api.github.com/users/baxterthehacker",
                        "https://github.com/baxterthehacker",
                        "https://api.github.com/users/baxterthehacker/followers",
                        "https://api.github.com/users/baxterthehacker/following{/other_user}",
                        "https://api.github.com/users/baxterthehacker/gists{/gist_id}",
                        "https://api.github.com/users/baxterthehacker/starred{/owner}{/repo}",
                        "https://api.github.com/users/baxterthehacker/subscriptions",
                        "https://api.github.com/users/baxterthehacker/orgs",
                        "https://api.github.com/users/baxterthehacker/repos",
                        "https://api.github.com/users/baxterthehacker/events{/privacy}",
                        "https://api.github.com/users/baxterthehacker/received_events",
                        "User",
                        false
                )
        );
        final String expected = MAPPER.writeValueAsString(MAPPER.readValue(fixture("fixtures/models/github_webhook.json"), GithubWebhook.class));

        assertThat(MAPPER.writeValueAsString(githubWebhook)).isEqualTo(expected);
    }

    @Test
    public void deserializesFromJSON() throws Exception {
        final GithubWebhook githubWebhook = new GithubWebhook(
                "refs/heads/changes",
                "9049f1265b7d61be4a8904a9a27120d2064dab3b",
                "0d1a26e67d8f5eaf1f6ba5c57fc3c7d91ac0fd1c",
                false,
                false,
                false,
                null,
                "https://github.com/baxterthehacker/public-repo/compare/9049f1265b7d...0d1a26e67d8f",
                new ArrayList<Commit>() {{
                    add(new Commit(
                                    "0d1a26e67d8f5eaf1f6ba5c57fc3c7d91ac0fd1c",
                                    "f9d2a07e9488b91af2641b26b9407fe22a451433",
                                    true,
                                    "Update README.md",
                                    "2015-05-05T19:40:15-04:00",
                                    "https://github.com/baxterthehacker/public-repo/commit/0d1a26e67d8f5eaf1f6ba5c57fc3c7d91ac0fd1c",
                                    new Author(
                                            "baxterthehacker",
                                            "baxterthehacker@users.noreply.github.com",
                                            "baxterthehacker"
                                    ),
                                    new Committer(
                                            "baxterthehacker",
                                            "baxterthehacker@users.noreply.github.com",
                                            "baxterthehacker"
                                    ),
                                    new ArrayList<>(),
                                    new ArrayList<>(),
                                    new ArrayList<String>() {{
                                        add("README.md");
                                    }}
                            )
                    );
                }},
                new HeadCommit(
                        "0d1a26e67d8f5eaf1f6ba5c57fc3c7d91ac0fd1c",
                        "f9d2a07e9488b91af2641b26b9407fe22a451433",
                        true,
                        "Update README.md",
                        "2015-05-05T19:40:15-04:00",
                        "https://github.com/baxterthehacker/public-repo/commit/0d1a26e67d8f5eaf1f6ba5c57fc3c7d91ac0fd1c",
                        new Author(
                                "baxterthehacker",
                                "baxterthehacker@users.noreply.github.com",
                                "baxterthehacker"
                        ),
                        new Committer(
                                "baxterthehacker",
                                "baxterthehacker@users.noreply.github.com",
                                "baxterthehacker"
                        ),
                        new ArrayList<>(),
                        new ArrayList<>(),
                        new ArrayList<String>() {{
                            add("README.md");
                        }}),
                new Repository(
                        35129377,
                        "public-repo",
                        "baxterthehacker/public-repo",
                        new Owner(
                                "baxterthehacker",
                                "baxterthehacker@users.noreply.github.com"
                        ),
                        false,
                        "https://github.com/baxterthehacker/public-repo",
                        "",
                        false,
                        "https://github.com/baxterthehacker/public-repo",
                        "https://api.github.com/repos/baxterthehacker/public-repo/forks",
                        "https://api.github.com/repos/baxterthehacker/public-repo/keys{/key_id}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/collaborators{/collaborator}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/teams",
                        "https://api.github.com/repos/baxterthehacker/public-repo/hooks",
                        "https://api.github.com/repos/baxterthehacker/public-repo/issues/events{/number}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/events",
                        "https://api.github.com/repos/baxterthehacker/public-repo/assignees{/user}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/branches{/branch}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/tags",
                        "https://api.github.com/repos/baxterthehacker/public-repo/git/blobs{/sha}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/git/tags{/sha}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/git/refs{/sha}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/git/trees{/sha}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/statuses/{sha}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/languages",
                        "https://api.github.com/repos/baxterthehacker/public-repo/stargazers",
                        "https://api.github.com/repos/baxterthehacker/public-repo/contributors",
                        "https://api.github.com/repos/baxterthehacker/public-repo/subscribers",
                        "https://api.github.com/repos/baxterthehacker/public-repo/subscription",
                        "https://api.github.com/repos/baxterthehacker/public-repo/commits{/sha}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/git/commits{/sha}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/comments{/number}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/issues/comments{/number}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/contents/{+path}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/compare/{base}...{head}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/merges",
                        "https://api.github.com/repos/baxterthehacker/public-repo/{archive_format}{/ref}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/downloads",
                        "https://api.github.com/repos/baxterthehacker/public-repo/issues{/number}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/pulls{/number}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/milestones{/number}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/notifications{?since,all,participating}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/labels{/name}",
                        "https://api.github.com/repos/baxterthehacker/public-repo/releases{/id}",
                        1430869212,
                        "2015-05-05T23:40:12Z",
                        1430869217,
                        "git://github.com/baxterthehacker/public-repo.git",
                        "git@github.com:baxterthehacker/public-repo.git",
                        "https://github.com/baxterthehacker/public-repo.git",
                        "https://github.com/baxterthehacker/public-repo",
                        null,
                        0,
                        0,
                        0,
                        null,
                        true,
                        true,
                        true,
                        true,
                        0,
                        null,
                        0,
                        0,
                        0,
                        0,
                        "master",
                        0,
                        "master"
                ),
                new Pusher(
                        "baxterthehacker",
                        "baxterthehacker@users.noreply.github.com"
                ),
                new Sender(
                        "baxterthehacker",
                        6752317,
                        "https://avatars.githubusercontent.com/u/6752317?v=3",
                        "",
                        "https://api.github.com/users/baxterthehacker",
                        "https://github.com/baxterthehacker",
                        "https://api.github.com/users/baxterthehacker/followers",
                        "https://api.github.com/users/baxterthehacker/following{/other_user}",
                        "https://api.github.com/users/baxterthehacker/gists{/gist_id}",
                        "https://api.github.com/users/baxterthehacker/starred{/owner}{/repo}",
                        "https://api.github.com/users/baxterthehacker/subscriptions",
                        "https://api.github.com/users/baxterthehacker/orgs",
                        "https://api.github.com/users/baxterthehacker/repos",
                        "https://api.github.com/users/baxterthehacker/events{/privacy}",
                        "https://api.github.com/users/baxterthehacker/received_events",
                        "User",
                        false
                )
        );
        assertThat(MAPPER.readValue(fixture("fixtures/models/github_webhook.json"), GithubWebhook.class)).isEqualToComparingFieldByFieldRecursively(githubWebhook);
    }
}
