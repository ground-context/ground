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

package edu.berkeley.ground.resources;

import com.codahale.metrics.annotation.Timed;
import edu.berkeley.ground.api.models.gh.GithubWebhook;
import edu.berkeley.ground.api.models.gh.GithubWebhookFactory;
import edu.berkeley.ground.exceptions.GroundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("/githubwh")
@Produces(MediaType.APPLICATION_JSON)
public class GithubWebhookResource {
    private static final Logger LOGGER = LoggerFactory.getLogger(GithubWebhookResource.class);

    private GithubWebhookFactory githubWebhookFactory;
    private String resp;;

    public GithubWebhookResource(GithubWebhookFactory githubWebhookFactory, String resp) {
        this.resp = resp;
        this.githubWebhookFactory = githubWebhookFactory;
    }

/*
    @GET
    @Timed
    @Path("/test")
    public String testGet() throws GroundException {
        LOGGER.info("Checking if its working.");
        return resp;
    }
*/


    @POST
    @Timed
    @Path("/webhook")
    public GithubWebhook makeGithubWebhook(@Valid GithubWebhook githubWebhook) throws GroundException {
        LOGGER.info("Receiving github webhook");
        LOGGER.info("Ref:"+githubWebhook.getRef());
        LOGGER.info("Before:"+githubWebhook.getBefore());
        LOGGER.info("After:"+githubWebhook.getAfter());
        LOGGER.info("Created:"+githubWebhook.getCreated());
        LOGGER.info("Deleted:"+githubWebhook.getDeleted());
        LOGGER.info("Forced:"+githubWebhook.getForced());
        LOGGER.info("Baseref:"+githubWebhook.getBaseRef());
        LOGGER.info("Compare:"+githubWebhook.getCompare());
        LOGGER.info("Commits:"+githubWebhook.getCommits());
        LOGGER.info("Head commit:"+githubWebhook.getHeadCommit());
        LOGGER.info("Repo:"+githubWebhook.getRepository());
        LOGGER.info("Pusher:"+githubWebhook.getPusher());
        LOGGER.info("Sender:"+githubWebhook.getSender());

        return new GithubWebhook(githubWebhook.getRef(),
                githubWebhook.getBefore(),
                githubWebhook.getAfter(),
                githubWebhook.getCreated(),
                githubWebhook.getDeleted(),
                githubWebhook.getForced(),
                githubWebhook.getBaseRef(),
                githubWebhook.getCompare(),
                githubWebhook.getCommits(),
                githubWebhook.getHeadCommit(),
                githubWebhook.getRepository(),
                githubWebhook.getPusher(),
                githubWebhook.getSender());

        //TODO: use githubWebhookFactory
/*        return this.githubWebhookFactory.create(githubWebhook.getRef(),
                githubWebhook.getBefore(),
                githubWebhook.getAfter(),
                githubWebhook.getCreated(),
                githubWebhook.getDeleted(),
                githubWebhook.getForced(),
                githubWebhook.getBaseRef(),
                githubWebhook.getCompare(),
                githubWebhook.getCommits(),
                githubWebhook.getHeadCommit(),
                githubWebhook.getRepository(),
                githubWebhook.getPusher(),
                githubWebhook.getSender());*/

    }

/*    @POST
    @Timed
    @Path("/testparse")
    public GithubWebhook makeGithubWebhookTest(@Valid GithubWebhook githubWebhook) throws GroundException {
        LOGGER.info("Receiving test github webhook");
        return this.githubWebhookFactory.create(githubWebhook.getRef(),
                githubWebhook.getBefore(),
                githubWebhook.getAfter(),
                githubWebhook.getCreated(),
                githubWebhook.getDeleted(),
                githubWebhook.getForced(),
                githubWebhook.getBaseRef(),
                githubWebhook.getCompare(),
                githubWebhook.getCommits(),
                githubWebhook.getHeadCommit(),
                githubWebhook.getRepository(),
                githubWebhook.getPusher(),
                githubWebhook.getSender());
    }

    @POST
    @Timed
    @Path("/webhookString")
    public String createGithubString(@Valid String json) throws GroundException {
        LOGGER.info("Receiving github webhook");
        return json;
    }
*/

}
