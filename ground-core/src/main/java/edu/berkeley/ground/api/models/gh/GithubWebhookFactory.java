/**
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

package edu.berkeley.ground.api.models.gh;

import edu.berkeley.ground.exceptions.GroundException;

import java.util.List;
import java.util.Map;

public abstract class GithubWebhookFactory {
    public abstract GithubWebhook create(String ref,
                                         String before,
                                         String after,
                                         Boolean created,
                                         Boolean deleted,
                                         Boolean forced,
                                         String base_ref,
                                         String compare,
                                         List<Commit> commits,
                                         HeadCommit head_commit,
                                         Repository repository,
                                         Pusher pusher,
                                         Sender sender) throws GroundException;


    protected static GithubWebhook construct(String ref,
                                             String before,
                                             String after,
                                             Boolean created,
                                             Boolean deleted,
                                             Boolean forced,
                                             String base_ref,
                                             String compare,
                                             List<Commit> commits,
                                             HeadCommit head_commit,
                                             Repository repository,
                                             Pusher pusher,
                                             Sender sender) throws GroundException {
        return new GithubWebhook(ref, before, after, created, deleted, forced, base_ref, compare, commits, head_commit, repository, pusher, sender);
    }
}
